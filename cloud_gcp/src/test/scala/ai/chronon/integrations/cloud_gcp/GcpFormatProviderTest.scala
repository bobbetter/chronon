package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.submission.SparkSessionBuilder
import com.google.cloud.bigquery._
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.util

class GcpFormatProviderTest extends AnyFlatSpec with MockitoSugar {

  lazy val spark: SparkSession = SparkSessionBuilder.build(
    "GcpFormatProviderTest",
    local = true
  )

  it should "check getFormat works for URI's that have a wildcard in between" ignore {
    // todo(tchow): Remove this test, getting rid of this abstraction eventually.
    val gcpFormatProvider = new GcpFormatProvider(spark)
    val sourceUris = "gs://bucket-name/path/to/data/*.parquet"
    val tableName = "gs://bucket-name/path/to/data"

    // mocking because bigquery Table doesn't have a constructor
    val mockTable = mock[Table]
    when(mockTable.getDefinition).thenReturn(
      ExternalTableDefinition
        .newBuilder("external")
        .setSourceUris(util.Arrays.asList(sourceUris))
        .setHivePartitioningOptions(HivePartitioningOptions.newBuilder().setSourceUriPrefix(tableName).build())
        .setFormatOptions(FormatOptions.parquet())
        .build())
    when(mockTable.getTableId).thenReturn(TableId.of("project", "dataset", "table"))

    val gcsFormat = gcpFormatProvider.readFormat(tableName)
  }
}
