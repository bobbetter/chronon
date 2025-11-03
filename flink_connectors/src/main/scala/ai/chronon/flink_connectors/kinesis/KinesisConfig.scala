package ai.chronon.flink_connectors.kinesis

import ai.chronon.flink.FlinkUtils
import ai.chronon.online.TopicInfo
import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants

import java.util.Properties

object KinesisConfig {
  // AWS property keys
  val AwsRegion = "AWS_REGION"
  val AwsDefaultRegion = "AWS_DEFAULT_REGION"
  val AwsAccessKeyId = "AWS_ACCESS_KEY_ID"
  val AwsSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
  val KinesisEndpoint = "KINESIS_ENDPOINT"
  
  // Kinesis consumer property keys
  val TaskParallelism = "tasks"
  val InitialPosition = "initial_position"
  val EnableEfo = "enable_efo"
  val EfoConsumerName = "efo_consumer_name"

  val DefaultParallelism = 1

  private def getRequiredProperty(key: String, props: Map[String, String], topicInfo: TopicInfo): String =
    FlinkUtils.getProperty(key, props, topicInfo)
      .getOrElse(throw new IllegalArgumentException(s"Missing required property: $key"))

  private def getOptionalProperty(key: String, props: Map[String, String], topicInfo: TopicInfo): Option[String] =
    FlinkUtils.getProperty(key, props, topicInfo)

  /** Builds a Properties object with Kinesis consumer configuration.
    *
    * Required properties:
    * - AWS_REGION (or AWS_DEFAULT_REGION): The AWS region where the Kinesis stream exists
    * - AWS_ACCESS_KEY_ID: AWS access key for authentication
    * - AWS_SECRET_ACCESS_KEY: AWS secret access key for authentication
    *
    * Optional properties:
    * - KINESIS_ENDPOINT: Custom Kinesis endpoint (useful for local testing)
    * - initial_position: Starting position (LATEST, TRIM_HORIZON, or AT_TIMESTAMP)
    * - enable_efo: Enable Enhanced Fan-Out (EFO) for the consumer
    * - efo_consumer_name: Consumer name for EFO
    */
  def buildConsumerConfig(props: Map[String, String], topicInfo: TopicInfo): Properties = {
    val config = new Properties()

    // Configure AWS region (try AWS_REGION first, fall back to AWS_DEFAULT_REGION)
    val region = getOptionalProperty(AwsRegion, props, topicInfo)
      .orElse(getOptionalProperty(AwsDefaultRegion, props, topicInfo))
      .getOrElse(throw new IllegalArgumentException(s"Missing required property: $AwsRegion or $AwsDefaultRegion"))
    config.put(AWSConfigConstants.AWS_REGION, region)

    // Configure AWS credentials
    config.put(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC")
    config.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, getRequiredProperty(AwsAccessKeyId, props, topicInfo))
    config.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, getRequiredProperty(AwsSecretAccessKey, props, topicInfo))

    // Configure custom endpoint (optional, for local testing)
    getOptionalProperty(KinesisEndpoint, props, topicInfo)
      .foreach(config.put(AWSConfigConstants.AWS_ENDPOINT, _))

    // Configure initial position (defaults to LATEST)
    val initialPosition = getOptionalProperty(InitialPosition, props, topicInfo)
      .getOrElse(ConsumerConfigConstants.InitialPosition.LATEST.toString)
    config.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, initialPosition)

    // Configure Enhanced Fan-Out (EFO) if enabled
    getOptionalProperty(EnableEfo, props, topicInfo).foreach { efoEnabled =>
      val publisherType = if (efoEnabled.toBoolean) {
        ConsumerConfigConstants.RecordPublisherType.EFO
      } else {
        ConsumerConfigConstants.RecordPublisherType.POLLING
      }
      config.put(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, publisherType.toString)
    }

    // Configure EFO consumer name if provided
    getOptionalProperty(EfoConsumerName, props, topicInfo)
      .foreach(config.put(ConsumerConfigConstants.EFO_CONSUMER_NAME, _))

    config
  }
}