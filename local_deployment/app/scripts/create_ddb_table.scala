import ai.chronon.integrations.aws.DynamoDBKVStoreImpl
import ai.chronon.integrations.aws.DynamoDBKVStoreConstants
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.regions.Region
import java.net.URI

object CreateDdbTable {
  def main(args: Array[String]): Unit = {
    val tableName = sys.env.getOrElse("TABLE_NAME", sys.error("TABLE_NAME env var is required"))
    val endpoint  = sys.env.getOrElse("DYNAMO_ENDPOINT", "http://localhost:8000")
    val region    = sys.env.getOrElse("AWS_DEFAULT_REGION", "us-west-2")

    val client = DynamoDbClient
      .builder()
      .endpointOverride(URI.create(endpoint))
      .region(Region.of(region))
      .build()

    try {
      val kv = new DynamoDBKVStoreImpl(client)
      kv.create(tableName, Map(DynamoDBKVStoreConstants.isTimedSorted -> "true"))
      println(s"Ensured DynamoDB table exists: $tableName (is-time-sorted=true)")
    } finally {
      try client.close() catch { case _: Throwable => () }
    }
  }
}


