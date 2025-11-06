package ai.chronon.integrations.aws

import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.ChrononHudiKryoRegistrator
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.scalatest.flatspec.AnyFlatSpec

class HudiTableUtilsTest extends AnyFlatSpec {
  lazy val spark: SparkSession = SparkSessionBuilder
    .build(
      "HudiTableUtilsTest",
      local = true,
      additionalConfig = Some(
        Map(
          "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
          "spark.sql.extensions" -> "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
          "spark.chronon.table_write.format" -> "hudi",
          "spark.kryo.registrator" -> classOf[ChrononHudiKryoRegistrator].getName
        )
      )
    )
  private val tableUtils = TableUtils(spark)

  // todo(tchow): Fix once we support HUDI
  it should "create a hudi table and read the hudi table" ignore {
    import spark.implicits._
    val tableName = "db.test_create_table"

    try {
      spark.sql("CREATE DATABASE IF NOT EXISTS db")
      val source = Seq(
        ("a", "2025-03-12"),
        ("b", "2025-03-12"),
        ("c", "2025-03-12"),
        ("d", "2025-03-12")
      )
      val sourceDF = source.toDF("id", "ds")

      tableUtils.createTable(sourceDF, tableName, fileFormat = "PARQUET", partitionColumns = List("ds"))
      assertTrue(spark.catalog.tableExists(tableName))
      val provider = spark
        .sql(s"DESCRIBE FORMATTED $tableName")
        .filter("col_name = 'Provider'")
        .collect()
        .head
        .getString(1)
      assertEquals("hudi", provider)

      tableUtils.insertPartitions(sourceDF, tableName)

      val back = tableUtils.loadTable(tableName)
      val backSet = back.select("id", "ds").as[(String, String)].collect().toSet
      assertEquals(source.toSet, backSet)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

}
