package ai.chronon.integrations.cloud_azure

import ai.chronon.spark.catalog.{DefaultFormatProvider, Format, Iceberg}
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCatalog

import scala.util.{Success, Try}

/** Azure format provider that checks for Iceberg tables and defaults to Snowflake.
  *
  * To use this provider, set the Spark config:
  *   spark.chronon.table.format_provider.class=ai.chronon.integrations.cloud_azure.AzureFormatProvider
  */
class AzureFormatProvider(override val sparkSession: SparkSession) extends DefaultFormatProvider(sparkSession) {

  override def readFormat(tableName: String): Option[Format] = {
    val parsedCatalog = Format.getCatalog(tableName)(sparkSession)
    val identifier = toIdentifierNoCatalog(tableName)
    val catalog = sparkSession.sessionState.catalogManager.catalog(parsedCatalog)

    catalog match {
      case sparkCatalog: SparkCatalog =>
        Try(sparkCatalog.loadTable(identifier)) match {
          case Success(_: SparkTable) =>
            logger.info(s"AzureFormatProvider: Detected Iceberg table $tableName")
            Some(Iceberg)
          case _ =>
            Some(Snowflake)
        }
      case tableCatalog: TableCatalog =>
        Try(tableCatalog.loadTable(identifier)) match {
          case Success(_: SparkTable) =>
            logger.info(s"AzureFormatProvider: Detected Iceberg table $tableName")
            Some(Iceberg)
          case _ =>
            Some(Snowflake)
        }
      case _ =>
        Some(Snowflake)
    }
  }

}
