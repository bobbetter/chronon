package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.submission.ChrononKryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.JavaSerializer
import org.apache.iceberg.gcp.gcs.GCSFileIO

class ChrononIcebergKryoRegistrator extends ChrononKryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    super.registerClasses(kryo)

    // Have not been able to get kryo serialization to work with the closure in the GCSFileIO class.
    // See: https://github.com/apache/iceberg/blob/cc4fe4cc50043ccba89700f7948090ff87a5baee/gcp/src/main/java/org/apache/iceberg/gcp/gcs/GCSFileIO.java#L138-L173
    // There are unit tests for this in the iceberg project: https://github.com/apache/iceberg/blob/cc4fe4cc50043ccba89700f7948090ff87a5baee/gcp/src/test/java/org/apache/iceberg/gcp/gcs/GCSFileIOTest.java#L201-L209
    // However for some reason this still fails when we run for real. Should consider testing this again once we
    // bump iceberg versions. To test, we simply remove this line and run any integration job that writes iceberg to GCS.
    kryo.register(classOf[GCSFileIO], new JavaSerializer)

    val additionalClassNames = Seq(
      "org.apache.iceberg.DataFile",
      "org.apache.iceberg.FileContent",
      "org.apache.iceberg.FileFormat",
      "org.apache.iceberg.GenericDataFile",
      "org.apache.iceberg.PartitionData",
      "org.apache.iceberg.SerializableByteBufferMap",
      "org.apache.iceberg.SerializableTable$SerializableConfSupplier",
      "org.apache.iceberg.SnapshotRef",
      "org.apache.iceberg.SnapshotRefType",
      "org.apache.iceberg.encryption.PlaintextEncryptionManager",
      "org.apache.iceberg.gcp.GCPProperties",
      "org.apache.iceberg.hadoop.HadoopFileIO",
      "org.apache.iceberg.hadoop.HadoopMetricsContext",
      "org.apache.iceberg.MetadataTableType",
      "org.apache.iceberg.io.ResolvingFileIO",
      "org.apache.iceberg.spark.source.SerializableTableWithSize",
      "org.apache.iceberg.spark.source.SerializableTableWithSize$SerializableMetadataTableWithSize",
      "org.apache.iceberg.spark.source.SparkWrite$TaskCommit",
      "org.apache.iceberg.types.Types$DateType",
      "org.apache.iceberg.types.Types$NestedField",
      "org.apache.iceberg.types.Types$StringType",
      "org.apache.iceberg.types.Types$StructType",
      "org.apache.iceberg.util.SerializableMap"
    )
    additionalClassNames.foreach(name => doRegister(name, kryo))
  }
}
