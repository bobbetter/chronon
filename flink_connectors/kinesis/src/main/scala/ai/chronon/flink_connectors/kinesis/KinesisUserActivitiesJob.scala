package ai.chronon.flink_connectors.kinesis

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema
import org.rogach.scallop.{ScallopConf, ScallopOption, Serialization}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{DefaultCredentialsProvider, WebIdentityTokenFileCredentialsProvider}
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.{GetSchemaVersionRequest, SchemaId, SchemaVersionNumber}
import software.amazon.awssdk.regions.Region

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt

/** Simple Flink job that reads user activities from a Kinesis stream with Glue schema registry
  * and emits periodic event counts. Used for testing EMR / EKS submission.
  *
  * Example usage:
  * --stream-name user-activities
  * --registry-name zipline-canary
  * --schema-name user-activities
  * --region us-west-2
  */
object KinesisUserActivitiesJob {
  private val logger = LoggerFactory.getLogger(getClass)

  class JobArgs(args: Seq[String]) extends ScallopConf(args) with Serialization {
    val streamName: ScallopOption[String] =
      opt[String](required = true, descr = "Kinesis stream name to read from")
    val registryName: ScallopOption[String] =
      opt[String](required = true, descr = "Glue Schema Registry name")
    val schemaName: ScallopOption[String] =
      opt[String](required = true, descr = "Schema name in Glue Registry")
    val region: ScallopOption[String] =
      opt[String](required = true, descr = "AWS region (e.g., us-west-2)")
    val checkpointDir: ScallopOption[String] =
      opt[String](required = false,
                  descr = "S3 path for checkpoints",
                  default = Some("s3://zipline-warehouse-canary/flink-checkpoints"))

    verify()
  }

  def main(args: Array[String]): Unit = {
    val jobArgs = new JobArgs(args)
    val streamName = jobArgs.streamName()
    val registryName = jobArgs.registryName()
    val schemaName = jobArgs.schemaName()
    val region = jobArgs.region()
    val checkpointDir = jobArgs.checkpointDir()

    logger.info(s"Starting KinesisUserActivitiesJob")
    logger.info(s"Stream: $streamName")
    logger.info(s"Registry: $registryName")
    logger.info(s"Schema: $schemaName")
    logger.info(s"Region: $region")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Configure restart strategy - limit restarts to prevent infinite crash loops
    // This will attempt to restart up to 5 times with exponential backoff, then fail the job
    env.setRestartStrategy(
      RestartStrategies.failureRateRestart(
        5, // max failures per interval
        Time.minutes(10), // time interval for measuring failure rate
        Time.seconds(30) // delay between restart attempts
      )
    )

    // Configure checkpointing
    env.enableCheckpointing(10.seconds.toMillis, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5.seconds.toMillis)
    env.getCheckpointConfig.setCheckpointTimeout(5.minutes.toMillis)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(5)
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // Fetch Avro schema from Glue on the driver
    logger.info(s"Fetching schema from Glue Schema Registry: $registryName/$schemaName")

    // Use Web Identity Token credentials provider for IRSA (IAM Roles for Service Accounts)
    // This ensures we use the pod's service account role instead of the node's instance role
    val credentialsProvider = WebIdentityTokenFileCredentialsProvider.create()

    val glueClient = GlueClient
      .builder()
      .region(Region.of(region))
      .credentialsProvider(credentialsProvider)
      .build()
    val schemaRequest = GetSchemaVersionRequest
      .builder()
      .schemaId(SchemaId.builder().registryName(registryName).schemaName(schemaName).build())
      .schemaVersionNumber(SchemaVersionNumber.builder().latestVersion(true).build())
      .build()
    val schemaResponse = glueClient.getSchemaVersion(schemaRequest)
    val avroSchemaString = schemaResponse.schemaDefinition()
    glueClient.close()

    logger.info(s"Retrieved Avro schema: ${avroSchemaString.take(200)}...")

    // Simple deserialization schema that just reads raw bytes
    val deserSchema = new KinesisDeserializationSchema[Array[Byte]] {
      override def deserialize(recordValue: Array[Byte],
                               partitionKey: String,
                               seqNum: String,
                               approxArrivalTimestamp: Long,
                               stream: String,
                               shardId: String): Array[Byte] = recordValue

      override def getProducedType = org.apache.flink.api.common.typeinfo.TypeInformation.of(classOf[Array[Byte]])
    }

    // Configure Kinesis consumer with polling retry/backoff tuned to handle
    // ProvisionedThroughputExceededException on low-throughput dev streams.
    // Flink's default of 3 retries is too low; 10 retries with exponential backoff
    // gives the shard time to recover before the job fails and restarts.
    val consumerProps = new java.util.Properties()
    consumerProps.setProperty("aws.region", region)
    consumerProps.setProperty("flink.stream.initpos", "LATEST")
    consumerProps.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX, "100")
    consumerProps.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_RETRIES, "10")
    consumerProps.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE, "1000")
    consumerProps.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX, "30000")
    consumerProps.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT, "1.5")

    val kinesisConsumer = new FlinkKinesisConsumer[Array[Byte]](
      streamName,
      deserSchema,
      consumerProps
    )

    // Create data stream - deserialize Avro and count
    val stream = env
      .addSource(kinesisConsumer, s"Kinesis Source: $streamName")
      .uid("kinesis-source")
      .map(new AvroDeserializeFunction(avroSchemaString))
      .uid("avro-deserialize")
      .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks[String]())

    // Count events and log periodically
    stream
      .map(new EventCounterFunction())
      .uid("event-counter")
      .setParallelism(1)
      .filter(_ != null) // Filter out nulls from counter function
      .uid("filter-nulls")
      .print()
      .name("Print Event Counts")

    logger.info(s"Executing job: Kinesis User Activities Counter")
    env.execute("Kinesis User Activities Counter")
  }

  /** Deserializes Avro bytes to a string representation of the record
    */
  class AvroDeserializeFunction(avroSchemaString: String) extends RichMapFunction[Array[Byte], String] {
    @transient private lazy val avroSchema = new Schema.Parser().parse(avroSchemaString)
    @transient private lazy val reader = new GenericDatumReader[GenericRecord](avroSchema)

    override def map(bytes: Array[Byte]): String = {
      try {
        val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
        val record = reader.read(null, decoder)
        record.toString
      } catch {
        case e: Exception =>
          logger.error(s"Failed to deserialize Avro record", e)
          s"ERROR: ${e.getMessage}"
      }
    }
  }

  /** Counts events and logs statistics periodically
    */
  class EventCounterFunction extends RichMapFunction[String, String] {
    @transient private var eventCount: Long = 0
    @transient private var lastLogTime: Long = 0
    private val logIntervalMs = 10.seconds.toMillis

    override def map(record: String): String = {
      eventCount += 1
      val currentTime = System.currentTimeMillis()

      if (lastLogTime == 0) {
        lastLogTime = currentTime
      }

      if (currentTime - lastLogTime >= logIntervalMs) {
        val timeDiff = (currentTime - lastLogTime) / 1000.0
        val rate = eventCount / timeDiff
        val message = f"[Event Counter] Total events: $eventCount, Rate: $rate%.2f events/sec"
        logger.info(message)
        lastLogTime = currentTime
        eventCount = 0
        message
      } else {
        null // Don't emit on every event
      }
    }
  }
}
