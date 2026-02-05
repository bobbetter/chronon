package ai.chronon.integrations.cloud_azure

import ai.chronon.spark.catalog.{DefaultFormatProvider, Format, Iceberg}
import org.apache.spark.sql.SparkSession

/** Azure format provider that checks for Iceberg tables and defaults to Snowflake.
  *
  * To use this provider, set the Spark config:
  *   spark.chronon.table.format_provider.class=ai.chronon.integrations.cloud_azure.AzureFormatProvider
  */
class AzureFormatProvider(override val sparkSession: SparkSession) extends DefaultFormatProvider(sparkSession) {

  override def readFormat(tableName: String): Option[Format] = {
    if (isIcebergTable(tableName)) {
      logger.info(s"AzureFormatProvider: Detected Iceberg table $tableName")
      Some(Iceberg)
    } else {
      logger.info(s"AzureFormatProvider: Table $tableName is not Iceberg, defaulting to Snowflake")
      Some(Snowflake)
    }
  }

}
