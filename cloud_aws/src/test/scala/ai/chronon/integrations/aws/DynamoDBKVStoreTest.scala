package ai.chronon.integrations.aws

import ai.chronon.api.Constants.{ContinuationKey, ListLimit}
import ai.chronon.api.TilingUtils
import ai.chronon.online.KVStore._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import java.net.URI
import java.nio.charset.StandardCharsets
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

object DDBTestUtils {

  // different types of tables to store
  case class Model(modelId: String, modelName: String, online: Boolean)
  case class TimeSeries(joinName: String, featureName: String, tileTs: Long, metric: String, summary: Array[Double])

}

class DynamoDBKVStoreTest extends AnyFlatSpec with BeforeAndAfterAll {

  import DDBTestUtils._
  import DynamoDBKVStoreConstants._

  var dynamoContainer: GenericContainer[_] = _
  var client: DynamoDbClient = _
  var kvStoreImpl: DynamoDBKVStoreImpl = _

  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  def modelKeyEncoder(model: Model): Array[Byte] = {
    objectMapper.writeValueAsString(model.modelId).getBytes(StandardCharsets.UTF_8)
  }

  def modelValueEncoder(model: Model): Array[Byte] = {
    objectMapper.writeValueAsString(model).getBytes(StandardCharsets.UTF_8)
  }

  def timeSeriesKeyEncoder(timeSeries: TimeSeries): Array[Byte] = {
    objectMapper.writeValueAsString((timeSeries.joinName, timeSeries.featureName)).getBytes(StandardCharsets.UTF_8)
  }

  def timeSeriesValueEncoder(series: TimeSeries): Array[Byte] = {
    objectMapper.writeValueAsString(series).getBytes(StandardCharsets.UTF_8)
  }

  override def beforeAll(): Unit = {
    // Start the DynamoDB Local container
    dynamoContainer = new GenericContainer(DockerImageName.parse("amazon/dynamodb-local:latest"))
    dynamoContainer.withExposedPorts(8000: Integer)
    dynamoContainer.withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
    dynamoContainer.start()

    // Create the DynamoDbClient pointing to the container
    val dynamoEndpoint = s"http://${dynamoContainer.getHost}:${dynamoContainer.getMappedPort(8000)}"
    client = DynamoDbClient
      .builder()
      .endpointOverride(URI.create(dynamoEndpoint))
      .region(Region.US_WEST_2)
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create("dummy", "dummy")
        ))
      .build()
  }

  override def afterAll(): Unit = {
    if (client != null) {
      client.close()
    }
    if (dynamoContainer != null) {
      dynamoContainer.stop()
    }
  }

  // Test creation of a table with primary keys only (e.g. model)
  it should "create p key only table" in {
    val dataset = "models"
    val props = Map(isTimedSorted -> "false")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)
    // Verify that the table exists
    val tables = client.listTables().tableNames()
    tables.contains(dataset) shouldBe true
    // try another create for an existing table, should not fail
    kvStore.create(dataset, props)
  }

  // Test creation of a table with primary + sort keys (e.g. time series)
  it should "create p key and sort key table" in {
    val dataset = "timeseries"
    val props = Map(isTimedSorted -> "true")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)
    // Verify that the table exists
    val tables = client.listTables().tableNames()
    tables.contains(dataset) shouldBe true
    // here too, another create should not fail
    kvStore.create(dataset, props)
  }

  // Test table scan with pagination
  it should "table scan with pagination" in {
    val dataset = "models"
    val props = Map(isTimedSorted -> "false")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)

    val putReqs = (0 until 100).map { i =>
      val model = Model(s"my_model_$i", s"test model $i", online = true)
      buildModelPutRequest(model, dataset)
    }

    val putResults = Await.result(kvStore.multiPut(putReqs), 1.minute)
    putResults.length shouldBe putReqs.length
    putResults.foreach(r => r shouldBe true)

    // call list - first call is only for 10 elements
    val listReq1 = ListRequest(dataset, Map(ListLimit -> 10))
    val listResults1 = Await.result(kvStore.list(listReq1), 1.minute)
    listResults1.resultProps.contains(ContinuationKey) shouldBe true
    validateExpectedListResponse(listResults1.values, 10)

    // call list - with continuation key
    val listReq2 =
      ListRequest(dataset, Map(ListLimit -> 100, ContinuationKey -> listResults1.resultProps(ContinuationKey)))
    val listResults2 = Await.result(kvStore.list(listReq2), 1.minute)
    listResults2.resultProps.contains(ContinuationKey) shouldBe false
    validateExpectedListResponse(listResults2.values, 100)
  }

  // Test write & read of a simple blob dataset
  it should "blob data round trip" in {
    val dataset = "models"
    val props = Map(isTimedSorted -> "false")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)

    val model1 = Model("my_model_1", "test model 1", online = true)
    val model2 = Model("my_model_2", "test model 2", online = true)
    val model3 = Model("my_model_3", "test model 3", online = false)

    val putReq1 = buildModelPutRequest(model1, dataset)
    val putReq2 = buildModelPutRequest(model2, dataset)
    val putReq3 = buildModelPutRequest(model3, dataset)

    val putResults = Await.result(kvStore.multiPut(Seq(putReq1, putReq2, putReq3)), 1.minute)
    putResults shouldBe Seq(true, true, true)

    // let's try and read these
    val getReq1 = buildModelGetRequest(model1, dataset)
    val getReq2 = buildModelGetRequest(model2, dataset)
    val getReq3 = buildModelGetRequest(model3, dataset)

    val getResult1 = Await.result(kvStore.multiGet(Seq(getReq1)), 1.minute)
    val getResult2 = Await.result(kvStore.multiGet(Seq(getReq2)), 1.minute)
    val getResult3 = Await.result(kvStore.multiGet(Seq(getReq3)), 1.minute)

    validateExpectedModelResponse(model1, getResult1)
    validateExpectedModelResponse(model2, getResult2)
    validateExpectedModelResponse(model3, getResult3)
  }

  // Test write and query of a time series dataset
  it should "time series query" in {
    val dataset = "timeseries"
    val props = Map(isTimedSorted -> "true")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    val points = tsRange.map(ts => TimeSeries("my_join", "my_feature_1", ts, "my_metric", Array(1.0, 2.0, 3.0)))

    // write to the kv store and confirm the writes were successful
    val putRequests = points.map(p => buildTSPutRequest(p, dataset))
    val putResult = Await.result(kvStore.multiPut(putRequests), 1.minute)
    putResult.length shouldBe tsRange.length
    putResult.foreach(r => r shouldBe true)

    // query in time range: 10/05/24 00:00 to 10/10
    val getRequest1 = buildTSGetRequest(points.head, dataset, 1728086400000L, 1728518400000L)
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.minute)
    validateExpectedTimeSeriesResponse(points.head, 1728086400000L, 1728518400000L, getResult1)
  }

  private def buildModelPutRequest(model: Model, dataset: String): PutRequest = {
    val keyBytes = modelKeyEncoder(model)
    val valueBytes = modelValueEncoder(model)
    PutRequest(keyBytes, valueBytes, dataset, None)
  }

  private def buildModelGetRequest(model: Model, dataset: String): GetRequest = {
    val keyBytes = modelKeyEncoder(model)
    GetRequest(keyBytes, dataset, None, None)
  }

  private def buildTSPutRequest(timeSeries: TimeSeries, dataset: String): PutRequest = {
    val keyBytes = timeSeriesKeyEncoder(timeSeries)
    val valueBytes = timeSeriesValueEncoder(timeSeries)
    PutRequest(keyBytes, valueBytes, dataset, Some(timeSeries.tileTs))
  }

  private def buildTSGetRequest(timeSeries: TimeSeries, dataset: String, startTs: Long, endTs: Long): GetRequest = {
    val keyBytes = timeSeriesKeyEncoder(timeSeries)
    GetRequest(keyBytes, dataset, Some(startTs), Some(endTs))
  }

  private def validateExpectedModelResponse(expectedModel: Model, response: Seq[GetResponse]): Unit = {
    response.length shouldBe 1
    for (
      tSeq <- response.head.values;
      tv <- tSeq
    ) {
      tSeq.length shouldBe 1
      val jsonStr = new String(tv.bytes, StandardCharsets.UTF_8)
      val returnedModel = objectMapper.readValue(jsonStr, classOf[Model])
      returnedModel shouldBe expectedModel
    }
  }

  private def validateExpectedListResponse(response: Try[Seq[ListValue]], maxElements: Int): Unit = {
    response match {
      case Success(mSeq) =>
        mSeq.length <= maxElements shouldBe true
        mSeq.foreach { modelKV =>
          val jsonStr = new String(modelKV.valueBytes, StandardCharsets.UTF_8)
          val returnedModel = objectMapper.readValue(jsonStr, classOf[Model])
          val returnedKeyJsonStr = new String(modelKV.keyBytes, StandardCharsets.UTF_8)
          val returnedKey = objectMapper.readValue(returnedKeyJsonStr, classOf[String])
          returnedModel.modelId shouldBe returnedKey
        }
      case Failure(exception) =>
        fail(s"List response failed with exception: $exception")
    }
  }

  private def validateExpectedTimeSeriesResponse(expectedTSBase: TimeSeries,
                                                 startTs: Long,
                                                 endTs: Long,
                                                 response: Seq[GetResponse]): Unit = {
    response.length shouldBe 1
    val expectedTimeSeriesPoints = (startTs to endTs by 1.hour.toMillis).map(ts => expectedTSBase.copy(tileTs = ts))
    for (
      tSeq <- response.head.values;
      (tv, ev) <- tSeq.zip(expectedTimeSeriesPoints)
    ) {
      tSeq.length shouldBe expectedTimeSeriesPoints.length
      val jsonStr = new String(tv.bytes, StandardCharsets.UTF_8)
      val returnedTimeSeries = objectMapper.readValue(jsonStr, classOf[TimeSeries])
      // we just match the join name and timestamps for simplicity
      returnedTimeSeries.tileTs shouldBe ev.tileTs
      returnedTimeSeries.joinName shouldBe ev.joinName
    }
  }

  // Test write and query of a tiled streaming dataset
  // This simulates the Flink TiledGroupByJob writing tiles and the Fetcher reading them
  it should "tiled streaming data round trip" in {
    // Streaming tables end with _STREAMING suffix
    val dataset = "MY_GROUPBY_V1_STREAMING"
    val props = Map(isTimedSorted -> "true")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)

    val entityKeyBytes = "entity_key_123".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis

    // Simulate Flink writing tiles (like TiledAvroCodecFn does)
    // Each tile has a TileKey with tileStartTimestampMillis set
    val tileTimestamps = Seq(1728000000000L, 1728003600000L, 1728007200000L, 1728010800000L) // 4 hours of tiles
    val putRequests = tileTimestamps.map { tileStart =>
      val tileKey = TilingUtils.buildTileKey(dataset, entityKeyBytes, Some(tileSizeMillis), Some(tileStart))
      val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
      val valueBytes = s"tile_value_at_$tileStart".getBytes(StandardCharsets.UTF_8)
      PutRequest(tileKeyBytes, valueBytes, dataset, Some(tileStart))
    }

    val putResults = Await.result(kvStore.multiPut(putRequests), 1.minute)
    putResults.length shouldBe tileTimestamps.length
    putResults.foreach(r => r shouldBe true)

    // Simulate Fetcher reading tiles (like GroupByFetcher does)
    // The Fetcher builds a TileKey without tileStartTimestampMillis and queries by time range
    val readTileKey = TilingUtils.buildTileKey(dataset, entityKeyBytes, Some(tileSizeMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)

    // Query for tiles in range [tileTimestamps(1), tileTimestamps(3)]
    val startTs = tileTimestamps(1) // 1728003600000L
    val endTs = tileTimestamps(3) // 1728010800000L
    val getRequest = GetRequest(readKeyBytes, dataset, Some(startTs), Some(endTs))

    val getResults = Await.result(kvStore.multiGet(Seq(getRequest)), 1.minute)
    getResults.length shouldBe 1

    // Should get 3 tiles back (startTs, startTs+1hour, startTs+2hour)
    getResults.head.values match {
      case Success(timedValues) =>
        timedValues.length shouldBe 3
        // Verify the timestamps are in the expected range
        timedValues.foreach { tv =>
          (tv.millis >= startTs) shouldBe true
          (tv.millis <= endTs) shouldBe true
        }
      case Failure(ex) =>
        fail(s"Failed to read tiled streaming data: $ex")
    }
  }

  // Test that writes multiple tiles for different entity keys
  it should "handle multiple entity keys with tiled streaming data" in {
    val dataset = "MULTI_ENTITY_V1_STREAMING"
    val props = Map(isTimedSorted -> "true")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)

    val tileSizeMillis = 1.hour.toMillis
    val tileStart = 1728000000000L

    // Write tiles for two different entity keys
    val entityKey1 = "entity_key_1".getBytes(StandardCharsets.UTF_8)
    val entityKey2 = "entity_key_2".getBytes(StandardCharsets.UTF_8)

    val tileKey1 = TilingUtils.buildTileKey(dataset, entityKey1, Some(tileSizeMillis), Some(tileStart))
    val tileKey2 = TilingUtils.buildTileKey(dataset, entityKey2, Some(tileSizeMillis), Some(tileStart))

    val putReq1 = PutRequest(
      TilingUtils.serializeTileKey(tileKey1),
      "value_for_entity_1".getBytes(StandardCharsets.UTF_8),
      dataset,
      Some(tileStart)
    )
    val putReq2 = PutRequest(
      TilingUtils.serializeTileKey(tileKey2),
      "value_for_entity_2".getBytes(StandardCharsets.UTF_8),
      dataset,
      Some(tileStart)
    )

    val putResults = Await.result(kvStore.multiPut(Seq(putReq1, putReq2)), 1.minute)
    putResults shouldBe Seq(true, true)

    // Read back entity 1 only
    val readTileKey1 = TilingUtils.buildTileKey(dataset, entityKey1, Some(tileSizeMillis), None)
    val getRequest1 = GetRequest(TilingUtils.serializeTileKey(readTileKey1), dataset, Some(tileStart), Some(tileStart))
    val getResults1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.minute)

    getResults1.length shouldBe 1
    getResults1.head.values match {
      case Success(timedValues) =>
        timedValues.length shouldBe 1
        new String(timedValues.head.bytes, StandardCharsets.UTF_8) shouldBe "value_for_entity_1"
      case Failure(ex) =>
        fail(s"Failed to read entity 1: $ex")
    }

    // Read back entity 2 only
    val readTileKey2 = TilingUtils.buildTileKey(dataset, entityKey2, Some(tileSizeMillis), None)
    val getRequest2 = GetRequest(TilingUtils.serializeTileKey(readTileKey2), dataset, Some(tileStart), Some(tileStart))
    val getResults2 = Await.result(kvStore.multiGet(Seq(getRequest2)), 1.minute)

    getResults2.length shouldBe 1
    getResults2.head.values match {
      case Success(timedValues) =>
        timedValues.length shouldBe 1
        new String(timedValues.head.bytes, StandardCharsets.UTF_8) shouldBe "value_for_entity_2"
      case Failure(ex) =>
        fail(s"Failed to read entity 2: $ex")
    }
  }
}
