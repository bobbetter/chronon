package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.Extensions._
import ai.chronon.spark.catalog.{Format, TableUtils}
import com.google.cloud.bigquery._
import com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider
import org.apache.spark.sql.functions.{col, date_format, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.UUID
import scala.util.{Failure, Success, Try}

case class PartitionColumnNotFoundException(message: String) extends UnsupportedOperationException(message)
case class NativePartColumn(colName: String, isSystemDefined: Boolean)

case object BigQueryNative extends Format {

  private val bqFormat = classOf[Spark35BigQueryTableProvider].getName
  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  private lazy val bigQueryClient: BigQuery = bqOptions.getService

  private val internalBQPartitionCol = "__chronon_internal_bq_partition_col__"

  private def exportDataTemplate(uri: String, format: String, sql: String): String =
    f"""
      |EXPORT DATA
      |  OPTIONS (
      |    uri = '${uri}',
      |    format = '${format}',
      |    overwrite = false
      |    )
      |AS (
      |   ${sql}
      |);
      |""".stripMargin

  // TODO(tchow): use the cache flag
  override def table(tableName: String, partitionFilters: String, cacheDf: Boolean = false)(implicit
      sparkSession: SparkSession): DataFrame = {

    // First, need to clean the spark-based table name for the bigquery queries below.
    val bqTableId = SparkBQUtils.toTableId(tableName)
    val providedProject = scala.Option(bqTableId.getProject).getOrElse(bqOptions.getProjectId)
    val bqFriendlyName = f"${providedProject}.${bqTableId.getDataset}.${bqTableId.getTable}"

    // Next, check to see if the table has a pseudo partition column
    val pColOption = getPartitionColumn(providedProject, bqTableId)

    val partitionWheres = if (partitionFilters.nonEmpty) s"WHERE ${partitionFilters}" else partitionFilters
    val formatStr = "parquet"
    val select = pColOption match {
      case Some(nativeCol) if nativeCol.isSystemDefined =>
        s"SELECT ${nativeCol.colName} as ${internalBQPartitionCol}, * FROM ${bqFriendlyName} ${partitionWheres}"
      case _ => s"SELECT * FROM ${bqFriendlyName} ${partitionWheres}"
    }
    val catalogName = Format.getCatalog(tableName)
    val destPath = destPrefix(catalogName, tableName, formatStr)
    val exportSQL = exportDataTemplate(
      uri = destPath,
      format = formatStr,
      sql = select
    )

    logger.info(s"Starting BigQuery export job for table: ${bqFriendlyName}")
    val exportJobTry: Try[Job] = Try {
      val exportConf = QueryJobConfiguration.of(exportSQL)
      val job = bigQueryClient.create(JobInfo.of(exportConf))
      scala.Option(job.waitFor()).getOrElse(throw new RuntimeException("Export job returned null"))
    }

    exportJobTry.flatMap { job =>
      scala.Option(job.getStatus.getError) match {
        case Some(err) => Failure(new RuntimeException(s"BigQuery export job failed: $err"))
        case None      => Success(job)
      }
    } match {
      case Success(_) =>
        val internalLoad = sparkSession.read.format(formatStr).load(destPath)
        pColOption
          .map { case (nativeColumn) => // as long as we have a native partition column we'll attempt to rename it.
            internalLoad
              .withColumnRenamed(internalBQPartitionCol, nativeColumn.colName)
          }
          .getOrElse(internalLoad)

      case Failure(e) =>
        throw e
    }
  }

  private[cloud_gcp] def destPrefix(catalogName: String,
                                    tableName: String,
                                    formatStr: String,
                                    uniqueId: scala.Option[String] = None)(implicit sparkSession: SparkSession) = {
    val warehouseLocation = sparkSession.sessionState.conf
      .getConfString(s"spark.sql.catalog.${catalogName}.warehouse")
      .stripSuffix("/")
    val uuid = uniqueId.getOrElse(UUID.randomUUID().toString)
    s"${warehouseLocation}/export/${tableName.sanitize}_${uuid}/*.${formatStr}"

  }

  private def getPartitionColumn(projectId: String, bqTableId: TableId)(implicit
      sparkSession: SparkSession): scala.Option[NativePartColumn] = {
    import sparkSession.implicits._
    val partColsSql =
      s"""
         |SELECT column_name, is_system_defined FROM `${projectId}.${bqTableId.getDataset}.INFORMATION_SCHEMA.COLUMNS`
         |WHERE table_name = '${bqTableId.getTable}' AND is_partitioning_column = 'YES'
         |
         |""".stripMargin

    val pColOption = sparkSession.read
      .format(bqFormat)
      .option("project", projectId)
      // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
      // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations
      .option("viewsEnabled", true)
      .option("materializationDataset", bqTableId.getDataset)
      .load(partColsSql)
      .as[(String, String)]
      .collect
      .map { case (name, isSystemDefined) =>
        NativePartColumn(
          name,
          isSystemDefined = isSystemDefined match {
            case "YES" => true
            case "NO"  => false
            case _ =>
              throw new IllegalArgumentException(s"Unknown partition column system definition: ${isSystemDefined}")
          }
        )
      }
      .headOption

    pColOption
  }

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] = {
    val tableIdentifier = SparkBQUtils.toTableId(tableName)
    val definition = scala
      .Option(bigQueryClient.getTable(tableIdentifier))
      .map((table) => table.getDefinition.asInstanceOf[TableDefinition])
      .getOrElse(throw new IllegalArgumentException(s"Table ${tableName} does not exist."))

    definition match {
      case view: ViewDefinition => {
        if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
          throw new NotImplementedError("subPartitionsFilter is not supported on this format.")
        }
        import sparkSession.implicits._

        val tableIdentifier = SparkBQUtils.toTableId(tableName)
        val providedProject = scala.Option(tableIdentifier.getProject).getOrElse(bqOptions.getProjectId)
        val partitionWheres = if (partitionFilters.nonEmpty) s"WHERE ${partitionFilters}" else partitionFilters

        val bqPartSQL =
          s"""
             |select distinct ${partitionColumn} FROM ${tableName} ${partitionWheres}
             |""".stripMargin

        val partVals = sparkSession.read
          .format(bqFormat)
          .option("project", providedProject)
          // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
          // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations
          .option("viewsEnabled", true)
          .option("materializationDataset", tableIdentifier.getDataset)
          .load(bqPartSQL)

        partVals.as[String].collect().toList
      }
      case std: StandardTableDefinition =>
        super.primaryPartitions(tableName, partitionColumn, partitionFilters, subPartitionsFilter)
      case other =>
        throw new IllegalArgumentException(
          s"Table ${tableName} is not a view or standard table. It is of type ${other.getClass.getName}."
        )
    }
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    import sparkSession.implicits._
    val tableIdentifier = SparkBQUtils.toTableId(tableName)
    val providedProject = scala.Option(tableIdentifier.getProject).getOrElse(bqOptions.getProjectId)
    val table = tableIdentifier.getTable
    val database =
      scala
        .Option(tableIdentifier.getDataset)
        .getOrElse(throw new IllegalArgumentException(s"database required for table: ${tableName}"))

    // See: https://cloud.google.com/bigquery/docs/information-schema-columns
    val partColsSql =
      s"""
           |SELECT column_name FROM `${providedProject}.${database}.INFORMATION_SCHEMA.COLUMNS`
           |WHERE table_name = '${table}' AND is_partitioning_column = 'YES'
           |
           |""".stripMargin

    val partitionCol = sparkSession.read
      .format(bqFormat)
      .option("project", providedProject)
      // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
      // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations
      .option("viewsEnabled", true)
      .option("materializationDataset", database)
      .load(partColsSql)
      .as[String]
      .collect
      .headOption
      .getOrElse(throw PartitionColumnNotFoundException(s"No partition column for table ${tableName} found."))

    // See: https://cloud.google.com/bigquery/docs/information-schema-partitions
    val partValsSql =
      s"""
           |SELECT partition_id FROM `${providedProject}.${database}.INFORMATION_SCHEMA.PARTITIONS`
           |WHERE table_name = '${table}'
           |
           |""".stripMargin

    // TODO: remove temporary hack. this is done because the existing raw data is in the date format yyyy-MM-dd
    //  but partition values in bigquery's INFORMATION_SCHEMA.PARTITIONS are in yyyyMMdd format.
    //  moving forward, for bigquery gcp we should default to storing raw data in yyyyMMdd format.
    val partitionFormat = TableUtils(sparkSession).partitionFormat

    val partitionInfoDf = sparkSession.read
      .format(bqFormat)
      .option("project", providedProject)
      // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
      // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations
      .option("viewsEnabled", true)
      .option("materializationDataset", database)
      .load(partValsSql)

    val unfilteredDf = partitionInfoDf
      .select(
        date_format(
          to_date(
            col("partition_id"),
            "yyyyMMdd" // Note: this "yyyyMMdd" format is hardcoded but we need to change it to be something else.
          ),
          partitionFormat)
          .as(partitionCol))
      .na // Should filter out '__NULL__' and '__UNPARTITIONED__'. See: https://cloud.google.com/bigquery/docs/partitioned-tables#date_timestamp_partitioned_tables
      .drop()

    val partitionVals = (if (partitionFilters.isEmpty) {
                           unfilteredDf
                         } else {
                           unfilteredDf.where(partitionFilters)
                         })
      .as[String]
      .collect
      .toList

    partitionVals.map((p) => Map(partitionCol -> p))

  }

  override def supportSubPartitionsFilter: Boolean = false
}
