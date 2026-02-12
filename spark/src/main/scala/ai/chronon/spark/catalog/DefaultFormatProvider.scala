package ai.chronon.spark.catalog

import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Success, Try}

/** Default format provider implementation based on default Chronon supported open source library versions.
  */
class DefaultFormatProvider(val sparkSession: SparkSession) extends FormatProvider {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Checks the format of a given table if it exists.
  override def readFormat(tableName: String): Option[Format] = {
    Option(if (isIcebergTable(tableName)) {
      Iceberg
    } else if (isDeltaTable(tableName)) {
      DeltaLake
    } else if (sparkSession.catalog.tableExists(tableName)) {
      Hive
    } else { null })
  }

  protected def isIcebergTable(tableName: String): Boolean = {
    val parsedCatalog = Format.getCatalog(tableName)(sparkSession)
    val identifier = toIdentifierNoCatalog(tableName)
    val catalog = sparkSession.sessionState.catalogManager.catalog(parsedCatalog)

    catalog match {
      case sparkCatalog: SparkCatalog =>
        Try(sparkCatalog.loadTable(identifier)) match {
          case Success(_: SparkTable) =>
            logger.info(s"IcebergCheck: Detected iceberg formatted table $tableName.")
            true
          case _ =>
            logger.info(s"IcebergCheck: Checked table $tableName is not iceberg format.")
            false
        }
      case tableCatalog: TableCatalog =>
        Try(tableCatalog.loadTable(identifier)) match {
          case Success(_: SparkTable) =>
            logger.info(s"IcebergCheck: Detected iceberg formatted table $tableName.")
            true
          case _ =>
            logger.info(s"IcebergCheck: Checked table $tableName is not iceberg format.")
            false
        }
      case _ =>
        logger.info(s"IcebergCheck: Checked table $tableName is not iceberg format.")
        false
    }
  }

  protected def toIdentifierNoCatalog(tableName: String): Identifier = {
    val parsed = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    parsed.toList match {
      case _ :: namespace :: table :: Nil => Identifier.of(Array(namespace), table)
      case namespace :: table :: Nil      => Identifier.of(Array(namespace), table)
      case table :: Nil                   => Identifier.of(Array.empty, table)
      case _                              => throw new IllegalArgumentException(s"Invalid table name: $tableName")
    }
  }

  private def isDeltaTable(tableName: String): Boolean = {
    Try {
      val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
      describeResult.select("format").first().getString(0).toLowerCase
    } match {
      case Success(format) =>
        logger.info(s"Delta check: Successfully read the format of table: $tableName as $format")
        format == "delta"
      case _ =>
        // the describe detail calls fails for Delta Lake tables
        logger.info(s"Delta check: Unable to read the format of the table $tableName using DESCRIBE DETAIL")
        false
    }
  }
}
