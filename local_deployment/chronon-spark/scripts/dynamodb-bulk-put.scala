import ai.chronon.api.{Constants, GroupBy, ThriftJsonCodec}
import ai.chronon.api.Extensions.{MetadataOps, StringOps}
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.{Row, SparkSession}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._
import java.net.URI
import java.io.File
import java.nio.file.Paths
import scala.jdk.CollectionConverters._

// Required params (Spark conf only):
//   spark.chronon.bulkput.confPath (path under /srv/chronon/app pointing to compiled GroupBy conf)
// Optional ds:
//   spark.chronon.bulkput.ds
// Example:
// spark-shell \
//   --jars /srv/chronon/jars/chronon-spark-assembly.jar,/srv/chronon/jars/chronon-aws-assembly.jar \
//   --conf spark.chronon.bulkput.confPath=compiled/group_bys/quickstart/logins.v1__1 \
//   --conf spark.chronon.bulkput.ds=2025-10-17 \
//   -i /srv/chronon/scripts/dynamodb-bulk-put.scala
val region = sys.env.getOrElse("AWS_DEFAULT_REGION", "us-west-2")
val endpoint = sys.env.getOrElse("DYNAMO_ENDPOINT", "http://localhost:8000")
val confBase = "/srv/chronon/app"

val session = SparkSession.getActiveSession.getOrElse {
  val created = SparkSession.builder().appName("chronon-dynamodb-bulk-put").getOrCreate()
  SparkSession.setActiveSession(created)
  SparkSession.setDefaultSession(created)
  created
}

def resolveOptional(confKey: String): Option[String] =
  session.conf.getOption(confKey).filter(_.nonEmpty)

case class DerivedTables(sourceTable: String, datasetName: String)

def resolveConfFile(path: String): File = {
  val direct = new File(path)
  if (direct.exists()) return direct.getAbsoluteFile
  val candidate = Paths.get(confBase, path).toFile
  if (candidate.exists()) return candidate.getAbsoluteFile
  println(s"[dynamodb-bulk-put] ERROR: Unable to find conf '$path' under current directory or $confBase.")
  System.exit(1)
  candidate
}

val confPath = session.conf
  .getOption("spark.chronon.bulkput.confPath")
  .filter(_.nonEmpty)
  .getOrElse {
    println(s"[dynamodb-bulk-put] ERROR: Missing conf path. Provide via Spark conf 'spark.chronon.bulkput.confPath'.")
    System.exit(1)
    ""
}

def deriveTablesFromConf(confPath: String): DerivedTables = {
  val file = resolveConfFile(confPath)
  println(s"[dynamodb-bulk-put] Loading GroupBy conf from ${file.getAbsolutePath}")
  val groupBy = ThriftJsonCodec.fromJsonFile[GroupBy](file.getAbsolutePath, check = false)
  val sourceTable = groupBy.metaData.uploadTable
  val datasetName = groupBy.metaData.name
  println(s"[dynamodb-bulk-put] Derived source table: $sourceTable; dataset name: $datasetName")
  DerivedTables(sourceTable, datasetName)
}

val derivedTables = deriveTablesFromConf(confPath)
val sourceOfflineTable = derivedTables.sourceTable
val destinationDataset = derivedTables.datasetName
val ds = resolveOptional("spark.chronon.bulkput.ds").orNull
val driverClient = DynamoDbClient.builder().endpointOverride(URI.create(endpoint)).region(Region.of(region)).build()

case class PutRecord(keyBytes: Array[Byte], valueBytes: Array[Byte], dataset: String, tsMillis: Option[Long])

def mapDatasetName(dataset: String): String = {
  if (dataset == null) return null
  val target =
    if (dataset.endsWith("_BATCH") || dataset.endsWith("_STREAMING")) dataset
    else dataset.sanitize.toUpperCase + "_BATCH"
  println(s"[dynamodb-bulk-put] Name of target dataset is in KV: $target")
  target
}

def ensureTableExists(tableName: String): Unit = {
  val waiter = driverClient.waiter()
  val describeRequest = DescribeTableRequest.builder().tableName(tableName).build()
  val exists = try {
    driverClient.describeTable(describeRequest)
    true
  } catch {
    case _: ResourceNotFoundException => false
  }

  if (!exists) {
    val throughput = ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build()
    
    // Always create non-time-sorted tables - works for now with untiled jobs, 
    // but has to be revised for tiled jobs potentially.
    val attributeDefs = List(
      AttributeDefinition.builder().attributeName("keyBytes").attributeType(ScalarAttributeType.B).build()
    )
    val keySchema = List(
      KeySchemaElement.builder().attributeName("keyBytes").keyType(KeyType.HASH).build()
    )
    val createRequest = CreateTableRequest
      .builder()
      .tableName(tableName)
      .attributeDefinitions(attributeDefs.asJava)
      .keySchema(keySchema.asJava)
      .provisionedThroughput(throughput)
      .build()
    println(s"[dynamodb-bulk-put] Creating DynamoDB table $tableName")
    driverClient.createTable(createRequest)
    val waiterResponse = waiter.waitUntilTableExists(describeRequest)
    if (waiterResponse.matched().exception().isPresent) {
      throw waiterResponse.matched().exception().get()
    }
    println(s"[dynamodb-bulk-put] Created table $tableName")
  } else {
    println(s"[dynamodb-bulk-put] Table $tableName already exists")
  }
}

try {
  val tableUtils = TableUtils(session)
  val partitionFilter = Option(ds).map(v => s"WHERE ds = '$v'").getOrElse("")
  val offlineDf = tableUtils.sql(s"SELECT * FROM $sourceOfflineTable")
  val tsColumn =
    if (offlineDf.columns.contains(Constants.TimeColumn)) Constants.TimeColumn
    else s"(unix_timestamp(ds, 'yyyy-MM-dd') * 1000 + ${tableUtils.partitionSpec.spanMillis})"

  val df = tableUtils.sql(s"""
    |SELECT key_bytes, value_bytes, $tsColumn as ts
    |FROM $sourceOfflineTable
    |$partitionFilter
    |""".stripMargin)

  println(
    s"[dynamodb-bulk-put] Loaded number of records from source table: '${df.count()}'"
  )
  val targetDataset = mapDatasetName(destinationDataset)
  ensureTableExists(targetDataset)

  val defaultBatchSize = sys.env.getOrElse("DDB_BULKPUT_BATCH_SIZE", "100").toInt
  val requests = df.rdd.map { row: Row =>
    val key = row.get(0).asInstanceOf[Array[Byte]]
    val value = row.get(1).asInstanceOf[Array[Byte]]
    val timestamp = row.get(2).asInstanceOf[Long]
    PutRecord(key, value, targetDataset, Option(timestamp))
  }
  println(
    s"[dynamodb-bulk-put] Starting bulkPut from '$sourceOfflineTable' to '$destinationDataset' ds=${Option(ds).getOrElse("<all>")}."
  )
  requests.foreachPartition { it =>
    val execClient = DynamoDbClient.builder().endpointOverride(URI.create(endpoint)).region(Region.of(region)).build()
    val batch = new scala.collection.mutable.ArrayBuffer[PutRecord](defaultBatchSize)

    def putBatch(): Unit = {
      if (batch.isEmpty) return
      batch.foreach { rec =>
        val attributes = scala.collection.mutable.Map[String, AttributeValue](
          "keyBytes" -> AttributeValue.builder().b(SdkBytes.fromByteArray(rec.keyBytes)).build(),
          "valueBytes" -> AttributeValue.builder().b(SdkBytes.fromByteArray(rec.valueBytes)).build()
        )
        rec.tsMillis.foreach { ts =>
          attributes += Constants.TimeColumn -> AttributeValue.builder().n(ts.toString).build()
        }
        val request = PutItemRequest.builder().tableName(rec.dataset).item(attributes.asJava).build()
        execClient.putItem(request)
      }
      batch.clear()
    }

    try {
      it.foreach { rec =>
        batch += rec
        if (batch.size >= defaultBatchSize) putBatch()
      }
      putBatch()
    } finally {
      try execClient.close()
      catch { case _: Throwable => () }
    }
  }

  println(s"[dynamodb-bulk-put] bulkPut completed successfully.")
} catch {
  case t: Throwable =>
    println(s"[dynamodb-bulk-put] ERROR: bulkPut failed - ${t.getMessage}")
    t.printStackTrace()
    System.exit(1)
} finally {
  try driverClient.close()
  catch { case _: Throwable => () }
}

System.exit(0)
