package ai.chronon.spark.upload

import ai.chronon.spark.IonWriter
import ai.chronon.spark.utils.SparkTestBase
import com.amazon.ion.system.IonSystemBuilder
import com.amazon.ion.{IonBlob, IonDecimal, IonStruct, IonText}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll

import java.io.{File, FileInputStream}
import java.net.URI
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.time.LocalDate
import scala.jdk.CollectionConverters._

class IonWriterTest extends SparkTestBase with Matchers with BeforeAndAfterAll {

  private val tmpDir = Files.createTempDirectory("ion-writer-test").toFile

  override protected def sparkConfs: Map[String, String] = Map(
    "spark.sql.warehouse.dir" -> new File(tmpDir, "warehouse").getAbsolutePath
  )

  override def afterAll(): Unit = {
    try {
      if (tmpDir.exists()) {
        tmpDir.listFiles().foreach(_.delete())
        tmpDir.delete()
      }
    } finally super.afterAll()
  }

  behavior of "IonWriter"

  it should "write ion files with expected rows and fields" in {
    val partitionValue = "2025-10-17"
    val tsValue = "2025-10-17T00:00:00Z"
    val tsValueMillis = Instant.parse(tsValue).toEpochMilli
    val dataSetName = new File(tmpDir, "ion-output").getAbsolutePath

    val schema = StructType(
      Seq(
        StructField("key_bytes", BinaryType, nullable = true),
        StructField("value_bytes", BinaryType, nullable = true),
        StructField("key_json", StringType, nullable = true),
        StructField("value_json", StringType, nullable = true),
        StructField("ds", DateType, nullable = false)
      )
    )

    val rows = Seq(
      Row("k1".getBytes("UTF-8"), "v1-bytes".getBytes("UTF-8"), "k1-json", """{"v":"one"}""", LocalDate.parse(partitionValue)),
      Row("k2".getBytes("UTF-8"), "v2-bytes".getBytes("UTF-8"), "k2-json", """{"v":"two"}""", LocalDate.parse(partitionValue))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, numSlices = 2), schema)
    val paths = IonWriter.write(df, dataSetName, "ds", partitionValue)

    paths should not be empty
    all(paths) should include(s"ds=$partitionValue")

    val ion = IonSystemBuilder.standard().build()

    val parsed =
      paths.flatMap { p =>
        val path =
          if (p.startsWith("file:")) Paths.get(new URI(p)) // handle fully-qualified file URIs
          else Paths.get(p) // hadoop Path.toString() returns a filesystem path without a scheme
        Files.exists(path) shouldBe true
        val datagram = ion.getLoader.load(new FileInputStream(path.toFile))
        datagram.iterator().asScala.map { value =>
          val struct = value.asInstanceOf[IonStruct].get("Item").asInstanceOf[IonStruct]
          val keyBytes = Option(struct.get("keyBytes")).map(_.asInstanceOf[IonBlob].getBytes)
          val valueBytes = Option(struct.get("valueBytes")).map(_.asInstanceOf[IonBlob].getBytes)
          val ts = Option(struct.get("ts")).map(_.asInstanceOf[IonDecimal])
          (keyBytes, valueBytes, ts)
        }
      }

    parsed.size shouldBe rows.size
    parsed.map(_._1.get.toSeq).toSet should contain("k1".getBytes("UTF-8").toSeq)
    parsed.map(_._2.get.toSeq).toSet should contain("v2-bytes".getBytes("UTF-8").toSeq)
    parsed.flatMap(_._3).foreach(_.bigDecimalValue().longValueExact() shouldBe tsValueMillis)
  }

  it should "honor upload bucket when provided" in {
    val partitionValue = "2025-10-18"
    val dataSetName = "ion-output-bucket"
    val bucketDir = new File(tmpDir, "bucket-root")
    val uploadBucket = Some(bucketDir.toURI.toString)

    val schema = StructType(
      Seq(
        StructField("key_bytes", BinaryType, nullable = true),
        StructField("value_bytes", BinaryType, nullable = true),
        StructField("key_json", StringType, nullable = true),
        StructField("value_json", StringType, nullable = true),
        StructField("ds", DateType, nullable = false)
      )
    )

    val rows = Seq(
      Row("k3".getBytes("UTF-8"), "v3-bytes".getBytes("UTF-8"), "k3-json", """{"v":"three"}""", LocalDate.parse(partitionValue))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, numSlices = 1), schema)

    val paths = IonWriter.write(df, dataSetName, "ds", partitionValue, uploadBucket)

    paths should not be empty
    val expectedBase = new File(bucketDir, dataSetName).getAbsolutePath
    all(paths.map(p => new File(new URI(p)).getAbsolutePath)) should startWith(expectedBase + File.separator + s"ds=$partitionValue")
  }

  it should "normalize bucket values" in {
    IonWriter.cleanPath("test-bucket") shouldBe Some("s3://test-bucket")
    IonWriter.cleanPath("s3://already/present") shouldBe Some("s3://already/present")
    IonWriter.cleanPath("file:/tmp/somewhere") shouldBe Some("file:/tmp/somewhere")
    IonWriter.cleanPath("  ") shouldBe None
  }

  it should "resolve base path with and without bucket" in {
    val pathNoBucket = IonWriter.resolvePath("base/path", None)
    pathNoBucket shouldBe new Path("base/path")

    val bucketUri = new File(tmpDir, "bucket-resolve").toURI.toString
    val pathWithBucket = IonWriter.resolvePath("/inner/base", Some(bucketUri))
    pathWithBucket.toString shouldBe new Path(bucketUri).suffix("/inner/base").toString
  }
}
