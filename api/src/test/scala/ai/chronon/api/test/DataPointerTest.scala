package ai.chronon.api.test

import ai.chronon.api.DataPointer
import ai.chronon.api.URIDataPointer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataPointerTest extends AnyFlatSpec with Matchers {

  "DataPointer.apply" should "parse a simple s3 path" in {
    val result = DataPointer("s3://bucket/path/to/data.parquet")
    result should be(URIDataPointer("s3://bucket/path/to/data.parquet", Some("parquet"), Some("parquet"), Map.empty))
  }

  it should "parse a bigquery table with options" in {
    val result = DataPointer("bigquery(option1=value1,option2=value2)://project-id.dataset.table")
    result should be(
      URIDataPointer("project-id.dataset.table",
                     Some("bigquery"),
                     Some("bigquery"),
                     Map("option1" -> "value1", "option2" -> "value2")))
  }

  it should "parse a bigquery table without options" in {
    val result = DataPointer("bigquery://project-id.dataset.table")
    result should be(URIDataPointer("project-id.dataset.table", Some("bigquery"), Some("bigquery"), Map.empty))
  }

  it should "parse a kafka topic" in {
    val result = DataPointer("kafka://my-topic")
    result should be(URIDataPointer("my-topic", Some("kafka"), Some("kafka"), Map.empty))
  }

  it should "parse a file path with format" in {
    val result = DataPointer("file://path/to/data.csv")
    result should be(URIDataPointer("file://path/to/data.csv", Some("csv"), Some("csv"), Map.empty))
  }

  it should "parse options with spaces" in {
    val result = DataPointer("hive(key1 = value1, key2 = value2)://database.table")
    result should be(
      URIDataPointer("database.table", Some("hive"), Some("hive"), Map("key1" -> "value1", "key2" -> "value2")))
  }

  it should "handle paths with dots" in {
    val result = DataPointer("hdfs://path/to/data.with.dots.parquet")
    result should be(
      URIDataPointer("hdfs://path/to/data.with.dots.parquet", Some("parquet"), Some("parquet"), Map.empty))
  }

  it should "handle paths with multiple dots and no format" in {
    val result = DataPointer("file://path/to/data.with.dots")
    result should be(URIDataPointer("file://path/to/data.with.dots", Some("dots"), Some("dots"), Map.empty))
  }

  it should "handle paths with multiple dots and prefixed format" in {
    val result = DataPointer("file+csv://path/to/data.with.dots")
    result should be(URIDataPointer("file://path/to/data.with.dots", Some("csv"), Some("csv"), Map.empty))
  }

  it should "handle paths with format and pointer to folder with glob matching" in {
    val result = DataPointer("s3+parquet://path/to/*/*/")
    result should be(URIDataPointer("s3://path/to/*/*/", Some("parquet"), Some("parquet"), Map.empty))
  }

  it should "handle no catalog, just table" in {
    val result = DataPointer("namespace.table")
    result should be(URIDataPointer("namespace.table", None, None, Map.empty))
  }
}
