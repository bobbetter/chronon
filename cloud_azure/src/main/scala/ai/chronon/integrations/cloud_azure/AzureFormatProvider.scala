package ai.chronon.integrations.cloud_azure

import ai.chronon.spark.catalog.{DefaultFormatProvider, Format, Iceberg}
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{TableCatalog, Identifier}

import scala.util.{Failure, Success, Try}

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
          case Success(other) =>
            logger.info(s"AzureFormatProvider: Table $tableName is ${other.getClass.getName}, defaulting to Snowflake")
            Some(Snowflake)
          case Failure(e) =>
            logger.info(
              s"AzureFormatProvider: Could not load table $tableName: ${e.getMessage}, defaulting to Snowflake")
            Some(Snowflake)
        }
      case tableCatalog: TableCatalog =>
        Try(tableCatalog.loadTable(identifier)) match {
          case Success(_: SparkTable) =>
            logger.info(s"AzureFormatProvider: Detected Iceberg table $tableName")
            Some(Iceberg)
          case Success(other) =>
            logger.info(s"AzureFormatProvider: Table $tableName is ${other.getClass.getName}, defaulting to Snowflake")
            Some(Snowflake)
          case Failure(e) =>
            logger.info(
              s"AzureFormatProvider: Could not load table $tableName: ${e.getMessage}, defaulting to Snowflake")
            Some(Snowflake)
        }
      case _ =>
        logger.info(s"AzureFormatProvider: Unknown catalog type for $tableName, defaulting to Snowflake")
        Some(Snowflake)
    }
  }

  private def toIdentifierNoCatalog(tableName: String): Identifier = {
    val parsed = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    parsed.toList match {
      case _ :: namespace :: table :: Nil => Identifier.of(Array(namespace), table)
      case namespace :: table :: Nil      => Identifier.of(Array(namespace), table)
      case table :: Nil                   => Identifier.of(Array.empty, table)
      case _                              => throw new IllegalArgumentException(s"Invalid table name: $tableName")
    }
  }
}
