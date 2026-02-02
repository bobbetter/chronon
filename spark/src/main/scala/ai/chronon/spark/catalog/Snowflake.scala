package ai.chronon.spark.catalog

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Snowflake Format implementation for reading tables and partition metadata via JDBC.
  *
  * Configuration is read from Spark config:
  *   - spark.chronon.snowflake.jdbc.url: JDBC URL
  *   - spark.chronon.snowflake.token: Authentication token
  */
case object Snowflake extends Format {

  override def table(tableName: String, partitionFilters: String, cacheDf: Boolean = false)(implicit
      sparkSession: SparkSession): DataFrame = {
    throw new UnsupportedOperationException(
      "Direct table reads are not supported for Snowflake format. Use a stagingQuery with EngineType.SNOWFLAKE to export the data first.")
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    List.empty
  }

  override def supportSubPartitionsFilter: Boolean = false
}
