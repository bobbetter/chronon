package ai.chronon.flink_connectors.kinesis

import ai.chronon.flink.FlinkUtils
import ai.chronon.flink.source.FlinkSource
import ai.chronon.online.TopicInfo
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants
import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants

import java.util.Properties

/** Chronon Flink source that reads events from AWS Kinesis. Can be configured on the topic as:
  * kinesis://stream-name/tasks=20/<other-params>
  *
  * Config params such as the AWS region, access key ID, and secret access key are read from the
  * online properties (configured in teams.py or env variables passed via -Z flags).
  *
  * Kinesis differs from Kafka in a few aspects:
  * 1. Shard-based parallelism - we can derive parallelism based on the number of shards, but
  *    allow the user to override it via the 'tasks' property.
  * 2. Kinesis streams maintain their own checkpointing via shard iterators, so job restarts will
  *    resume from the last processed position.
  *
  * Required properties:
  * - AWS_REGION (or AWS_DEFAULT_REGION): The AWS region where the Kinesis stream exists
  * - AWS_ACCESS_KEY_ID: AWS access key for authentication
  * - AWS_SECRET_ACCESS_KEY: AWS secret access key for authentication
  *
  * Optional properties:
  * - tasks: Override the default parallelism
  * - KINESIS_ENDPOINT: Custom Kinesis endpoint (useful for local testing)
  * - initial_position: Starting position (LATEST, TRIM_HORIZON, or AT_TIMESTAMP)
  */
class KinesisFlinkSource[T](props: Map[String, String],
                             deserializationSchema: DeserializationSchema[T],
                             topicInfo: TopicInfo)
    extends FlinkSource[T] {

  import KinesisFlinkSource._

  implicit val parallelism: Int =
    FlinkUtils.getProperty(TaskParallelism, props, topicInfo).map(_.toInt).getOrElse(DefaultParallelism)

  val streamName: String = topicInfo.name

  override def getDataStream(topic: String, groupByName: String)(env: StreamExecutionEnvironment,
                                                                 parallelism: Int): SingleOutputStreamOperator[T] = {
    val consumerConfig = buildConsumerConfig(props, topicInfo)

    val kinesisConsumer = new FlinkKinesisConsumer[T](
      streamName,
      deserializationSchema,
      consumerConfig
    )

    // skip watermarks at the source as we derive them post Spark expr eval
    val noWatermarks: WatermarkStrategy[T] = WatermarkStrategy.noWatermarks()
    
    env
      .addSource(kinesisConsumer, s"Kinesis source: $groupByName - ${topicInfo.name}")
      .setParallelism(parallelism)
      .uid(s"kinesis-source-$groupByName")
      .assignTimestampsAndWatermarks(noWatermarks)
  }

  private def buildConsumerConfig(props: Map[String, String], topicInfo: TopicInfo): Properties = {
    val consumerConfig = new Properties()

    // AWS Region - required
    val region = FlinkUtils
      .getProperty(AwsRegion, props, topicInfo)
      .orElse(FlinkUtils.getProperty(AwsDefaultRegion, props, topicInfo))
      .getOrElse(throw new IllegalArgumentException(s"Missing required property: $AwsRegion or $AwsDefaultRegion"))
    consumerConfig.put(AWSConfigConstants.AWS_REGION, region)

    // AWS Credentials - use BASIC provider with access key and secret
    consumerConfig.put(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC")
    
    val accessKeyId = FlinkUtils
      .getProperty(AwsAccessKeyId, props, topicInfo)
      .getOrElse(throw new IllegalArgumentException(s"Missing required property: $AwsAccessKeyId"))
    consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, accessKeyId)

    val secretAccessKey = FlinkUtils
      .getProperty(AwsSecretAccessKey, props, topicInfo)
      .getOrElse(throw new IllegalArgumentException(s"Missing required property: $AwsSecretAccessKey"))
    consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, secretAccessKey)

    // Custom endpoint (for local testing with LocalStack or Kinesalite)
    FlinkUtils.getProperty(KinesisEndpoint, props, topicInfo).foreach { endpoint =>
      consumerConfig.put(AWSConfigConstants.AWS_ENDPOINT, endpoint)
    }

    // Initial position - defaults to LATEST
    val initialPosition = FlinkUtils
      .getProperty(InitialPosition, props, topicInfo)
      .getOrElse(ConsumerConfigConstants.InitialPosition.LATEST.toString)
    consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, initialPosition)

    // Enable EFO (Enhanced Fan-Out) if specified
    FlinkUtils.getProperty(EnableEfo, props, topicInfo).foreach { efoEnabled =>
      consumerConfig.put(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, 
        if (efoEnabled.toBoolean) ConsumerConfigConstants.RecordPublisherType.EFO.toString 
        else ConsumerConfigConstants.RecordPublisherType.POLLING.toString)
    }

    // Consumer name for EFO
    FlinkUtils.getProperty(EfoConsumerName, props, topicInfo).foreach { consumerName =>
      consumerConfig.put(ConsumerConfigConstants.EFO_CONSUMER_NAME, consumerName)
    }

    consumerConfig
  }
}

object KinesisFlinkSource {
  // Property keys
  val AwsRegion = "AWS_REGION"
  val AwsDefaultRegion = "AWS_DEFAULT_REGION"
  val AwsAccessKeyId = "AWS_ACCESS_KEY_ID"
  val AwsSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
  val KinesisEndpoint = "KINESIS_ENDPOINT"
  val TaskParallelism = "tasks"
  val InitialPosition = "initial_position"
  val EnableEfo = "enable_efo"
  val EfoConsumerName = "efo_consumer_name"

  // Default parallelism - go with a default of 10 as that gives us some room to handle
  // decent load without too many tasks
  val DefaultParallelism = 10
}

