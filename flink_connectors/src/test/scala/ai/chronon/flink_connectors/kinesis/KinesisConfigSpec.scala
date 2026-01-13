package ai.chronon.flink_connectors.kinesis

import ai.chronon.flink_connectors.kinesis.KinesisConfig.{ConsumerConfig, Defaults, Keys}
import ai.chronon.online.TopicInfo
import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KinesisConfigSpec extends AnyFlatSpec with Matchers {

  behavior of "KinesisConfig.buildConsumerConfig"

  it should "require a region or default region" in {
    val props = Map(
      Keys.AwsAccessKeyId -> "access",
      Keys.AwsSecretAccessKey -> "secret"
    )

    val topicInfo = TopicInfo("test-stream", "kinesis", Map.empty)

    an[IllegalArgumentException] should be thrownBy {
      KinesisConfig.buildConsumerConfig(props, topicInfo)
    }
  }

  it should "default optional values when not provided" in {
    val props = Map(
      Keys.AwsRegion -> "us-west-2",
      Keys.AwsAccessKeyId -> "access",
      Keys.AwsSecretAccessKey -> "secret"
    )

    val topicInfo = TopicInfo("test-stream", "kinesis", Map.empty)

    val kinesisConfig = KinesisConfig.buildConsumerConfig(props, topicInfo)
    

    kinesisConfig.parallelism shouldBe Defaults.Parallelism
    kinesisConfig.properties.getProperty(AWSConfigConstants.AWS_REGION) shouldBe "us-west-2"
    kinesisConfig.properties.getProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER) shouldBe "BASIC"
    kinesisConfig.properties.getProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION) shouldBe Defaults.InitialPosition
    kinesisConfig.properties.containsKey(AWSConfigConstants.AWS_ENDPOINT) shouldBe false
    kinesisConfig.properties.containsKey(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE) shouldBe false
    kinesisConfig.properties.containsKey(ConsumerConfigConstants.EFO_CONSUMER_NAME) shouldBe false
  }

  it should "apply overrides and optional fields from props and topic params" in {
    val props = Map(
      Keys.AwsAccessKeyId -> "access",
      Keys.AwsSecretAccessKey -> "secret",
      Keys.EnableEfo -> "true",
      Keys.TaskParallelism -> "7"
    )

    val topicParams = Map(
      Keys.AwsDefaultRegion -> "us-west-1",
      Keys.InitialPosition -> ConsumerConfigConstants.InitialPosition.TRIM_HORIZON.toString,
      Keys.KinesisEndpoint -> "http://localhost:4566",
      Keys.EfoConsumerName -> "consumer"
    )

    val topicInfo = TopicInfo("test-stream", "kinesis", topicParams)

    val kinesisConfig = KinesisConfig.buildConsumerConfig(props, topicInfo)

    kinesisConfig.parallelism shouldBe 7
    kinesisConfig.properties.getProperty(AWSConfigConstants.AWS_REGION) shouldBe "us-west-1"
    kinesisConfig.properties.getProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION) shouldBe ConsumerConfigConstants.InitialPosition.TRIM_HORIZON.toString
    kinesisConfig.properties.getProperty(AWSConfigConstants.AWS_ENDPOINT) shouldBe "http://localhost:4566"
    kinesisConfig.properties.getProperty(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE) shouldBe ConsumerConfigConstants.RecordPublisherType.EFO.toString
    kinesisConfig.properties.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME) shouldBe "consumer"
  }
}

