package ai.chronon.spark

import com.amazon.ion.IonType
import com.amazon.ion.system.IonBinaryWriterBuilder
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

import java.math.BigDecimal
import java.sql.Date
import java.time.{LocalDate, ZoneOffset}
import java.util.UUID
import scala.util.control.NonFatal

object IonWriter {
  private val logger = LoggerFactory.getLogger(getClass)

  def write(df: DataFrame,
            dataSetName: String,
            partitionColumn: String,
            partitionValue: String,
            rootPath: Option[String] = None): Seq[String] = {
    val serializableConf = new SerializableConfiguration(df.sparkSession.sparkContext.hadoopConfiguration)
    val schema = df.schema

    val resolvedPath = resolvePath(dataSetName, rootPath)
    val partitionPath = new Path(resolvedPath, s"$partitionColumn=$partitionValue")

    val requiredColumns = Seq("key_bytes", "value_bytes", partitionColumn)
    val missingColumns = requiredColumns.filterNot(schema.fieldNames.contains)
    if (missingColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"DataFrame schema for Ion upload is missing required column(s): ${missingColumns.mkString(", ")}")
    }

    val keyIdx = schema.fieldIndex("key_bytes")
    val valueIdx = schema.fieldIndex("value_bytes")
    val tsIdx = schema.fieldIndex(partitionColumn)

    val written = df.rdd.mapPartitionsWithIndex( (partitionId, iter) =>
      if (!iter.hasNext) Iterator.empty
      else {
        val unique = UUID.randomUUID().toString
        val filePath = new Path(partitionPath, s"part-$partitionId-$unique.ion")
        val fs = FileSystem.get(filePath.toUri, serializableConf.value)
        fs.mkdirs(partitionPath)
        val out = fs.create(filePath, true)
        val writer = IonBinaryWriterBuilder.standard().build(out)

        var rowCount = 0L
        var keyBytesTotal = 0L
        var valueBytesTotal = 0L

        try {
          iter.foreach { row =>
            writer.stepIn(IonType.STRUCT)
            writer.setFieldName("Item")
            writer.stepIn(IonType.STRUCT)
            if (!row.isNullAt(keyIdx)) {
              val bytes = row.getAs[Array[Byte]](keyIdx)
              writer.setFieldName("keyBytes")
              writer.writeBlob(bytes)
              keyBytesTotal += bytes.length
            }
            if (!row.isNullAt(valueIdx)) {
              val bytes = row.getAs[Array[Byte]](valueIdx)
              writer.setFieldName("valueBytes")
              writer.writeBlob(bytes)
              valueBytesTotal += bytes.length
            }
            if (!row.isNullAt(tsIdx)) {
              writer.setFieldName("ts")
              val millis = toMillis(row.get(tsIdx))
              writer.writeDecimal(millis)
            }
            writer.stepOut()
            writer.stepOut()
            rowCount += 1
          }
          writer.finish()
        } catch {
          case NonFatal(e) =>
            logger.error(s"Failed writing Ion file at $filePath", e)
            throw e
        } finally {
          writer.close()
          out.close()
        }
        Iterator.single((filePath.toString, rowCount, keyBytesTotal, valueBytesTotal))
      }
    ).collect()

    val totalRows = written.map(_._2).sum
    if (totalRows == 0L) {
      throw new RuntimeException("Ion upload produced zero rows.")
    }

    val totalKeyBytes = written.map(_._3).sum
    val totalValueBytes = written.map(_._4).sum
    logger.info(
      s"Wrote Ion files for partition $partitionValue at ${resolvedPath.toString} rows=$totalRows key_bytes=$totalKeyBytes value_bytes=$totalValueBytes"
    )
    written.map(_._1)
  }

  def resolvePath(dataSetName: String, rootPath: Option[String]): Path = {
    val cleanedDatasetName = Option(dataSetName).map(_.trim).getOrElse("").stripPrefix("/")
    
    rootPath.flatMap(cleanPath) match {
      case Some(bucket) =>
        val resolved = new Path(new Path(bucket), cleanedDatasetName)
        logger.info(s"Ion upload resolved basePath=$cleanedDatasetName bucket=$bucket resolved=$resolved")
        resolved
      case None =>
        new Path(cleanedDatasetName)
    }
  }

  def cleanPath(rootPath: String): Option[String] = {
    Option(rootPath)
      .map(_.trim.stripSuffix("/"))
      .filter(_.nonEmpty)
      .map { value =>
        if (value.matches("^[A-Za-z][A-Za-z0-9+.-]*:.*")) value else s"s3://$value"
      }
  }

  def toMillis(value: Any): BigDecimal = {
    value match {
      case null => throw new IllegalArgumentException("Partition column is blank; cannot write Ion timestamp")
      case date: Date =>
        BigDecimal.valueOf(date.toInstant.toEpochMilli)
      case localDate: LocalDate =>
        BigDecimal.valueOf(localDate.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli)
      case other =>
        throw new IllegalArgumentException(s"Unsupported partition type: ${other.getClass.getName}")
    }
  }
}
