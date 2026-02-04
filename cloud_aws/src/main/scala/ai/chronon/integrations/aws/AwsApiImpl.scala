package ai.chronon.integrations.aws

import ai.chronon.online._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import ai.chronon.online.serde._

import java.net.URI
import java.util

/** Implementation of Chronon's API interface for AWS. This is a work in progress and currently just covers the
  * DynamoDB based KV store implementation.
  */
class AwsApiImpl(conf: Map[String, String]) extends Api(conf) {

  // For now similar to GcpApiImpl, we have a flag store that relies on some hardcoded values.
  val tilingEnabledFlagStore: FlagStore = (flagName: String, _: util.Map[String, String]) => {
    if (flagName == FlagStoreConstants.TILING_ENABLED) {
      true
    } else {
      false
    }
  }

  // We set the flag store to always return true for tiling enabled
  setFlagStore(tilingEnabledFlagStore)

  @transient lazy val ddbClient: DynamoDbClient = {
    var builder = DynamoDbClient
      .builder()

    sys.env.get("AWS_DEFAULT_REGION").foreach { region =>
      try {
        builder.region(Region.of(region))
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(s"Invalid AWS region format: $region", e)
      }
    }
    sys.env.get("DYNAMO_ENDPOINT").foreach { endpoint =>
      try {
        builder = builder.endpointOverride(URI.create(endpoint))
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(s"Invalid DynamoDB endpoint URI: $endpoint", e)
      }
    }
    builder.build()

  }

  override def genKvStore: KVStore = {
    new DynamoDBKVStoreImpl(ddbClient, conf)
  }

  /** The stream decoder method in the AwsApi is currently unimplemented. This needs to be implemented before
    * we can spin up the Aws streaming Chronon stack
    */
  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): SerDe = ???

  /** The external registry extension is currently unimplemented. We'll need to implement this prior to spinning up
    * a fully functional Chronon serving stack in Aws
    * @return
    */
  @transient lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()
  override def externalRegistry: ExternalSourceRegistry = registry

  /** The logResponse method is currently unimplemented. We'll need to implement this prior to bringing up the
    * fully functional serving stack in Aws which includes logging feature responses to a stream for OOC
    */
  override def logResponse(resp: LoggableResponse): Unit = ()

  override def genMetricsKvStore(tableBaseName: String): KVStore = {
    new DynamoDBKVStoreImpl(ddbClient, conf)
  }

  override def genEnhancedStatsKvStore(tableBaseName: String): KVStore = ???
}
