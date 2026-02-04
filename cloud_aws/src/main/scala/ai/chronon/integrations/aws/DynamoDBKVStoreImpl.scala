package ai.chronon.integrations.aws

import ai.chronon.api.Constants
import ai.chronon.api.Constants.{ContinuationKey, ListLimit}
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{GroupBy, MetaData, TilingUtils}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.KVStore
import ai.chronon.spark.{IonPathConfig, IonWriter}
import ai.chronon.online.KVStore.GetResponse
import ai.chronon.online.KVStore.ListRequest
import ai.chronon.online.KVStore.ListResponse
import ai.chronon.online.KVStore.ListValue
import ai.chronon.online.KVStore.TimedValue
import ai.chronon.online.metrics.Metrics.Context
import ai.chronon.online.metrics.Metrics
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.ImportTableRequest
import software.amazon.awssdk.services.dynamodb.model.InputFormat
import software.amazon.awssdk.services.dynamodb.model.InputCompressionType
import software.amazon.awssdk.services.dynamodb.model.S3BucketSource
import software.amazon.awssdk.services.dynamodb.model.TableCreationParameters
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import software.amazon.awssdk.services.dynamodb.model.DescribeImportRequest
import software.amazon.awssdk.services.dynamodb.model.ImportStatus

import java.nio.charset.Charset
import java.time.Instant
import java.util
import scala.concurrent.Future
import scala.util.Success
import scala.util.Try

object DynamoDBKVStoreConstants {
  // Read capacity units to configure DynamoDB table with
  val readCapacityUnits = "read-capacity"

  // Write capacity units to configure DynamoDB table with
  val writeCapacityUnits = "write-capacity"

  // Optional field that indicates if this table is meant to be time sorted in Dynamo or not
  val isTimedSorted = "is-time-sorted"

  // Name of the partition key column to use
  val partitionKeyColumn = "keyBytes"

  // Name of the time sort key column to use
  val sortKeyColumn = Constants.TimeColumn

  // TODO: tune these
  val defaultReadCapacityUnits = 10L
  val defaultWriteCapacityUnits = 10L

  /** Streaming tables (suffix _STREAMING) use TileKey wrapping for tiled data. */
  def isStreamingTable(dataset: String): Boolean = dataset.endsWith("_STREAMING")

  case class TileKeyComponents(baseKeyBytes: Array[Byte], tileSizeMillis: Long, tileStartTimestampMillis: Option[Long])

  /** Unwraps a TileKey to extract the entity key for use as DynamoDB partition key.
    *
    * Streaming tables have two serialization layers:
    *   - Outer: Thrift (TileKey struct with dataset, keyBytes, tileSizeMs, tileStartTs)
    *   - Inner: Avro (entity key, e.g. customer_id, stored in TileKey.keyBytes)
    *
    * This method deserializes only the Thrift layer. The returned baseKeyBytes
    * remain Avro-encoded and are used directly as the DynamoDB partition key.
    */
  def extractTileKeyComponents(keyBytes: Array[Byte]): TileKeyComponents = {
    val tileKey = TilingUtils.deserializeTileKey(keyBytes)
    val baseKeyBytes = tileKey.keyBytes.toScala.map(_.toByte).toArray
    val tileSizeMs = tileKey.tileSizeMillis
    val tileStartTs = if (tileKey.isSetTileStartTimestampMillis) Some(tileKey.tileStartTimestampMillis) else None
    TileKeyComponents(baseKeyBytes, tileSizeMs, tileStartTs)
  }

  // Builds key with tileSizeMs to support tile layering
  def buildKeyWithTileSize(baseKeyBytes: Array[Byte], tileSizeMs: Long): Array[Byte] = {
    baseKeyBytes ++ s"#$tileSizeMs".getBytes(Charset.forName("UTF-8"))
  }
}

class DynamoDBKVStoreImpl(dynamoDbClient: DynamoDbClient, conf: Map[String, String] = Map.empty) extends KVStore {

  import DynamoDBKVStoreConstants._

  protected val metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("dynamodb")

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  override def create(dataset: String, props: Map[String, Any]): Unit = {
    val dbWaiter = dynamoDbClient.waiter
    val maybeSortKeys = props.get(isTimedSorted) match {
      case Some(value: String) if value.toLowerCase == "true" => Some(sortKeyColumn)
      case Some(value: Boolean) if value                      => Some(sortKeyColumn)
      case _                                                  => None
    }

    val keyAttributes =
      Seq(AttributeDefinition.builder.attributeName(partitionKeyColumn).attributeType(ScalarAttributeType.B).build) ++
        maybeSortKeys.map(k => AttributeDefinition.builder.attributeName(k).attributeType(ScalarAttributeType.N).build)

    val keySchema =
      Seq(KeySchemaElement.builder.attributeName(partitionKeyColumn).keyType(KeyType.HASH).build) ++
        maybeSortKeys.map(p => KeySchemaElement.builder.attributeName(p).keyType(KeyType.RANGE).build)

    val rcu = getCapacityUnits(props, readCapacityUnits, defaultReadCapacityUnits)
    val wcu = getCapacityUnits(props, writeCapacityUnits, defaultWriteCapacityUnits)

    val request =
      CreateTableRequest.builder
        .attributeDefinitions(keyAttributes.toList.toJava)
        .keySchema(keySchema.toList.toJava)
        .provisionedThroughput(ProvisionedThroughput.builder.readCapacityUnits(rcu).writeCapacityUnits(wcu).build)
        .tableName(dataset)
        .build

    logger.info(s"Triggering creation of DynamoDb table: $dataset")
    try {
      val _ = dynamoDbClient.createTable(request)
      val tableRequest = DescribeTableRequest.builder.tableName(dataset).build
      // Wait until the Amazon DynamoDB table is created.
      val waiterResponse = dbWaiter.waitUntilTableExists(tableRequest)
      if (waiterResponse.matched.exception().isPresent)
        throw waiterResponse.matched.exception().get()

      val tableDescription = waiterResponse.matched().response().get().table()
      logger.info(s"Table created successfully! Details: \n${tableDescription.toString}")
      metricsContext.increment("create.successes")
    } catch {
      case _: ResourceInUseException => logger.info(s"Table: $dataset already exists")
      case e: Exception =>
        logger.error(s"Error creating Dynamodb table: $dataset", e)
        metricsContext.increment("create.failures")
        throw e
    }
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    // partition our requests into pure get style requests (where we're missing timestamps and only have key lookup)
    // and query requests (we want to query a range based on afterTsMillis -> endTsMillis or now() )
    val (getLookups, queryLookups) = requests.partition(r => r.startTsMillis.isEmpty)
    val getItemRequestPairs = getLookups.map { req =>
      val keyAttributeMap = primaryKeyMap(req.keyBytes)
      (req, GetItemRequest.builder.key(keyAttributeMap.toJava).tableName(req.dataset).build)
    }

    // timestamp to use for all get responses when the underlying tables don't have a ts field
    val defaultTimestamp = Instant.now().toEpochMilli

    // get item results, requests where we're missing timestamps and only have key lookup
    val getItemResults = getItemRequestPairs.map { case (req, getItemReq) =>
      Future {
        val item: Try[util.Map[String, AttributeValue]] =
          handleDynamoDbOperation(metricsContext.withSuffix("multiget"), req.dataset) {
            dynamoDbClient.getItem(getItemReq).item()
          }

        val response = item.map(i => List(i).toJava)
        val resultValue: Try[Seq[TimedValue]] = extractTimedValues(response, defaultTimestamp)
        GetResponse(req, resultValue)
      }
    }

    // query requests, requests where we want to query a range based on afterTsMillis -> endTsMillis or now()
    val queryRequestPairs = queryLookups.map { req =>
      // For streaming tables, extract the Avro entity key from the TileKey wrapper
      // and include tileSizeMs in the partition key to support tile layering
      val partitionKeyBytes = if (isStreamingTable(req.dataset)) {
        val tileComponents = extractTileKeyComponents(req.keyBytes)
        buildKeyWithTileSize(tileComponents.baseKeyBytes, tileComponents.tileSizeMillis)
      } else {
        req.keyBytes
      }
      val queryRequest = buildTimeRangeQuery(req.dataset, partitionKeyBytes, req.startTsMillis.get, req.endTsMillis)
      (req, queryRequest)
    }

    val queryResults = queryRequestPairs.map { case (req, queryRequest) =>
      Future {
        val responses = handleDynamoDbOperation(metricsContext.withSuffix("query"), req.dataset) {
          dynamoDbClient.query(queryRequest).items()
        }
        val resultValue: Try[Seq[TimedValue]] = extractTimedValues(responses, defaultTimestamp)
        GetResponse(req, resultValue)
      }
    }

    Future.sequence(getItemResults ++ queryResults)
  }

  override def list(request: ListRequest): Future[ListResponse] = {
    val listLimit = request.props.get(ListLimit) match {
      case Some(value: Int)    => value
      case Some(value: String) => value.toInt
      case _                   => 100
    }

    val maybeExclusiveStartKey = request.props.get(ContinuationKey)
    val maybeExclusiveStartKeyAttribute = maybeExclusiveStartKey.map { k =>
      AttributeValue.builder.b(SdkBytes.fromByteArray(k.asInstanceOf[Array[Byte]])).build
    }

    val scanBuilder = ScanRequest.builder.tableName(request.dataset).limit(listLimit)
    val scanRequest = maybeExclusiveStartKeyAttribute match {
      case Some(value) => scanBuilder.exclusiveStartKey(Map(partitionKeyColumn -> value).toJava).build
      case _           => scanBuilder.build
    }

    Future {
      val tryScanResponse = handleDynamoDbOperation(metricsContext.withSuffix("list"), request.dataset) {
        dynamoDbClient.scan(scanRequest)
      }
      val resultElements = extractListValues(tryScanResponse)
      val noPagesLeftResponse = ListResponse(request, resultElements, Map.empty)
      val listResponse = tryScanResponse match {
        case Success(scanResponse) if scanResponse.hasLastEvaluatedKey =>
          val lastEvalKey = scanResponse.lastEvaluatedKey().toScala.get(partitionKeyColumn)
          lastEvalKey match {
            case Some(av) => ListResponse(request, resultElements, Map(ContinuationKey -> av.b().asByteArray()))
            case _        => noPagesLeftResponse
          }
        case _ => noPagesLeftResponse
      }

      listResponse
    }
  }

  // Dynamo has restrictions on the number of requests per batch (and the payload size) as well as some partial
  // success behavior on batch writes which necessitates a bit more logic on our end to tie things together.
  // To keep things simple for now, we implement the multiput as a sequence of put calls.
  override def multiPut(keyValueDatasets: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    logger.info(s"Triggering multiput for ${keyValueDatasets.size}: rows")
    val datasetToWriteRequests = keyValueDatasets.map { req =>
      // For streaming tables, unwrap TileKey to use entity key + tileSizeMs as partition key
      // and tileStartTs as sort key. Including tileSizeMs in the key supports tile layering.
      val (actualKeyBytes, actualTimestamp) = if (isStreamingTable(req.dataset) && req.tsMillis.isDefined) {
        val tileComponents = extractTileKeyComponents(req.keyBytes)
        val timestamp = tileComponents.tileStartTimestampMillis.getOrElse(req.tsMillis.get)
        val tiledKey = buildKeyWithTileSize(tileComponents.baseKeyBytes, tileComponents.tileSizeMillis)
        (tiledKey, Some(timestamp))
      } else {
        (req.keyBytes, req.tsMillis)
      }

      val attributeMap: Map[String, AttributeValue] = buildAttributeMap(actualKeyBytes, req.valueBytes)
      val tsMap =
        actualTimestamp
          .map(ts => Map(sortKeyColumn -> AttributeValue.builder.n(ts.toString).build))
          .getOrElse(Map.empty)

      val putItemReq =
        PutItemRequest.builder.tableName(req.dataset).item((attributeMap ++ tsMap).toJava).build()
      (req.dataset, putItemReq)
    }

    val futureResponses = datasetToWriteRequests.map { case (dataset, putItemRequest) =>
      Future {
        handleDynamoDbOperation(metricsContext.withSuffix("multiput"), dataset) {
          dynamoDbClient.putItem(putItemRequest)
        }.isSuccess
      }
    }
    Future.sequence(futureResponses)
  }

  /** Bulk loads data from S3 Ion files into DynamoDB using the ImportTable API.
    *
    * The Ion files are expected to have been written by IonWriter during GroupByUpload.
    * The S3 location is determined by IonWriter.resolveS3Location using:
    *   - Root path from config: spark.chronon.table_write.upload.root_path
    *   - Dataset name: sourceOfflineTable (e.g., namespace.groupby_v1__upload)
    *   - Partition column and value: ds={partition}
    *
    * Full path: s3://{spark.chronon.table_write.upload.root_path}/{sourceOfflineTable}/ds={partition}/
    */
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val rootPath = conf.get(IonPathConfig.UploadLocationKey)
    val partitionColumn = conf.getOrElse(IonPathConfig.PartitionColumnKey, IonPathConfig.DefaultPartitionColumn)

    // Use shared IonWriter path resolution to ensure consistency between producer and consumer
    val path = IonWriter.resolvePartitionPath(sourceOfflineTable, partitionColumn, partition, rootPath)
    val s3Source = toS3BucketSource(path)
    val groupBy = new GroupBy().setMetaData(new MetaData().setName(destinationOnlineDataSet))
    val tableName = groupBy.batchDataset
    logger.info(s"Starting DynamoDB import for table: $tableName from S3: $s3Source")

    val tableParams = TableCreationParameters
      .builder()
      .tableName(tableName)
      .keySchema(
        KeySchemaElement.builder().attributeName(partitionKeyColumn).keyType(KeyType.HASH).build()
      )
      .attributeDefinitions(
        AttributeDefinition.builder().attributeName(partitionKeyColumn).attributeType(ScalarAttributeType.B).build()
      )
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .build()

    val importRequest = ImportTableRequest
      .builder()
      .s3BucketSource(s3Source)
      .inputFormat(InputFormat.ION)
      .inputCompressionType(InputCompressionType.NONE)
      .tableCreationParameters(tableParams)
      .build()

    try {
      val startTs = System.currentTimeMillis()
      val importResponse = dynamoDbClient.importTable(importRequest)
      val importArn = importResponse.importTableDescription().importArn()

      logger.info(s"DynamoDB import initiated with ARN: $importArn for table: $tableName")

      // Wait for import to complete
      waitForImportCompletion(importArn, tableName)

      val duration = System.currentTimeMillis() - startTs
      logger.info(s"DynamoDB import completed for table: $tableName in ${duration}ms")
      metricsContext.increment("bulkPut.successes")
      metricsContext.distribution("bulkPut.latency", duration)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to import data to DynamoDB table: $tableName", e)
        metricsContext.increment("bulkPut.failures")
        throw e
    }
  }

  /** Converts a Hadoop Path to an S3BucketSource for DynamoDB ImportTable. */
  private def toS3BucketSource(path: org.apache.hadoop.fs.Path): S3BucketSource = {
    val uri = path.toUri
    S3BucketSource
      .builder()
      .s3Bucket(uri.getHost)
      .s3KeyPrefix(uri.getPath.stripPrefix("/") + "/")
      .build()
  }

  /** Waits for a DynamoDB import to complete by polling the import status. */
  private def waitForImportCompletion(importArn: String, tableName: String): Unit = {
    val maxWaitTimeMs = 30 * 60 * 1000L // 30 minutes
    val pollIntervalMs = 10 * 1000L // 10 seconds
    val startTime = System.currentTimeMillis()

    var status: ImportStatus = ImportStatus.IN_PROGRESS
    while (status == ImportStatus.IN_PROGRESS && (System.currentTimeMillis() - startTime) < maxWaitTimeMs) {
      Thread.sleep(pollIntervalMs)
      try {
        val describeRequest = DescribeImportRequest.builder().importArn(importArn).build()
        val describeResponse = dynamoDbClient.describeImport(describeRequest)
        status = describeResponse.importTableDescription().importStatus()
      } catch {
        case e: Exception =>
          logger.error(s"Error polling import status for $tableName", e)
          throw e
      }
      logger.info(s"DynamoDB import status for $tableName: $status")
    }

    status match {
      case ImportStatus.COMPLETED =>
        logger.info(s"DynamoDB import completed successfully for table: $tableName")
      case ImportStatus.FAILED | ImportStatus.CANCELLED =>
        throw new RuntimeException(s"DynamoDB import failed with status: $status for table: $tableName")
      case ImportStatus.IN_PROGRESS =>
        throw new RuntimeException(s"DynamoDB import timed out after ${maxWaitTimeMs}ms for table: $tableName")
      case _ =>
        logger.warn(s"Unknown import status: $status for table: $tableName")
    }
  }

  private def getCapacityUnits(props: Map[String, Any], key: String, defaultValue: Long): Long = {
    props.get(key) match {
      case Some(value: Long)   => value
      case Some(value: String) => value.toLong
      case _                   => defaultValue
    }
  }

  private def handleDynamoDbOperation[T](context: Context, dataset: String)(operation: => T): Try[T] = {
    Try {
      val startTs = System.currentTimeMillis()
      val result = operation
      context.distribution("latency", System.currentTimeMillis() - startTs)
      result
    }.recover {
      // log and emit metrics
      case e: ProvisionedThroughputExceededException =>
        logger.error(s"Provisioned throughput exceeded as we are low on IOPS on $dataset", e)
        context.increment("iops_error")
        throw e
      case e: ResourceNotFoundException =>
        logger.error(s"Unable to trigger operation on $dataset as its not found", e)
        context.increment("missing_table")
        throw e
      case e: Exception =>
        logger.error("Error interacting with DynamoDB", e)
        context.increment("dynamodb_error")
        throw e
    }
  }

  private def extractTimedValues(response: Try[util.List[util.Map[String, AttributeValue]]],
                                 defaultTimestamp: Long): Try[Seq[TimedValue]] = {
    response.map { ddbResponseList =>
      ddbResponseList.toScala.map { ddbResponseMap =>
        val responseMap = ddbResponseMap.toScala
        if (responseMap.isEmpty)
          throw new Exception("Empty response returned from DynamoDB")

        val valueBytes = responseMap.get("valueBytes").map(v => v.b().asByteArray())
        if (valueBytes.isEmpty)
          throw new Exception("DynamoDB response missing valueBytes")

        val timestamp = responseMap.get(sortKeyColumn).map(v => v.n().toLong).getOrElse(defaultTimestamp)
        TimedValue(valueBytes.get, timestamp)
      }
    }
  }

  private def extractListValues(tryScanResponse: Try[ScanResponse]): Try[Seq[ListValue]] = {
    tryScanResponse.map { response =>
      val ddbResponseList = response.items()
      ddbResponseList.toScala.map { ddbResponseMap =>
        val responseMap = ddbResponseMap.toScala
        if (responseMap.isEmpty)
          throw new Exception("Empty response returned from DynamoDB")

        val keyBytes = responseMap.get("keyBytes").map(v => v.b().asByteArray())
        val valueBytes = responseMap.get("valueBytes").map(v => v.b().asByteArray())

        if (keyBytes.isEmpty || valueBytes.isEmpty)
          throw new Exception("DynamoDB response missing key / valueBytes")
        ListValue(keyBytes.get, valueBytes.get)
      }
    }
  }

  private def primaryKeyMap(keyBytes: Array[Byte]): Map[String, AttributeValue] = {
    Map(partitionKeyColumn -> AttributeValue.builder.b(SdkBytes.fromByteArray(keyBytes)).build)
  }

  private def buildAttributeMap(keyBytes: Array[Byte], valueBytes: Array[Byte]): Map[String, AttributeValue] = {
    primaryKeyMap(keyBytes) ++
      Map(
        "valueBytes" -> AttributeValue.builder.b(SdkBytes.fromByteArray(valueBytes)).build
      )
  }

  /** Builds a DynamoDB query for a partition key with a time range on the sort key. */
  private def buildTimeRangeQuery(dataset: String,
                                  partitionKeyBytes: Array[Byte],
                                  startTs: Long,
                                  endTs: Option[Long]): QueryRequest = {
    val partitionAlias = "#pk"
    val timeAlias = "#ts"
    val attrNameAliasMap = Map(partitionAlias -> partitionKeyColumn, timeAlias -> sortKeyColumn)
    val endTsResolved = endTs.getOrElse(System.currentTimeMillis())
    val attrValuesMap = Map(
      ":partitionKeyValue" -> AttributeValue.builder.b(SdkBytes.fromByteArray(partitionKeyBytes)).build,
      ":start" -> AttributeValue.builder.n(startTs.toString).build,
      ":end" -> AttributeValue.builder.n(endTsResolved.toString).build
    )

    QueryRequest.builder
      .tableName(dataset)
      .keyConditionExpression(s"$partitionAlias = :partitionKeyValue AND $timeAlias BETWEEN :start AND :end")
      .expressionAttributeNames(attrNameAliasMap.toJava)
      .expressionAttributeValues(attrValuesMap.toJava)
      .build
  }
}
