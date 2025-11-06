package ai.chronon.flink.test.source

import ai.chronon.flink.source.{FlinkSourceProvider, KafkaFlinkSource}
import ai.chronon.online.TopicInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class KafkaFlinkSourceSpec extends AnyFlatSpec {

  it should "fail on missing kafka bootstrap servers" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)
    assertThrows[Exception] {
      FlinkSourceProvider.build(Map.empty, null, topicInfo)
    }
  }

  it should "read kafka bootstrap server from properties" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)
    val flinkSrc = FlinkSourceProvider.build(Map(KafkaFlinkSource.KafkaBootstrap -> "my-kafka-server"), null, topicInfo)

    flinkSrc.isInstanceOf[KafkaFlinkSource[_]] shouldBe true
    val kafkaSrc = flinkSrc.asInstanceOf[KafkaFlinkSource[_]]
    kafkaSrc.bootstrap shouldBe "my-kafka-server"
  }

  it should "read kafka bootstrap server from topic info" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map(KafkaFlinkSource.KafkaBootstrap -> "my-kafka-server"))
    val flinkSrc = FlinkSourceProvider.build(Map.empty, null, topicInfo)

    flinkSrc.isInstanceOf[KafkaFlinkSource[_]] shouldBe true
    val kafkaSrc = flinkSrc.asInstanceOf[KafkaFlinkSource[_]]
    kafkaSrc.bootstrap shouldBe "my-kafka-server"
  }

  it should "read kafka bootstrap server from host and port" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map("host" -> "my-host", "port" -> "9092"))
    val flinkSrc = FlinkSourceProvider.build(Map.empty, null, topicInfo)

    flinkSrc.isInstanceOf[KafkaFlinkSource[_]] shouldBe true
    val kafkaSrc = flinkSrc.asInstanceOf[KafkaFlinkSource[_]]
    kafkaSrc.bootstrap shouldBe "my-host:9092"
  }
}
