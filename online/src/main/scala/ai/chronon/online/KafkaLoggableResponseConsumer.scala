package ai.chronon.online

import ai.chronon.online.metrics.Metrics
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.slf4j.{Logger, LoggerFactory}

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.function.Consumer

class KafkaLoggableResponseConsumer(topicInfo: TopicInfo, maybeSchemaRegistryId: Option[Int])
    extends Consumer[LoggableResponse] {

  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val metricsContext: Metrics.Context =
    Metrics.Context(Metrics.Environment.JoinFetching).withSuffix("kafka_ooc")

  private lazy val kafkaProducer: KafkaProducer[String, Array[Byte]] = {
    val conf = topicInfo.params
    val bootstrap = conf.getOrElse("bootstrap", conf("host") + conf.get("port").map(":" + _).getOrElse(""))

    // create a Kafka producer
    val props = TopicChecker.mapToJavaProperties(conf ++ Map(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    new KafkaProducer[String, Array[Byte]](props)
  }

  override def accept(response: LoggableResponse): Unit = {
    val avroBytes = LoggableResponse.toAvroBytes(response)
    val avroBytesWithOptionalPreamble = {
      // tack on the schema registry magic byte and schema ID if provided
      // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
      maybeSchemaRegistryId
        .map { schemaId =>
          LoggableResponse.prependSchemaRegistryBytes(schemaId, avroBytes)
        }
        .getOrElse(avroBytes)
    }

    val key = response.joinName
    val record = new ProducerRecord[String, Array[Byte]](topicInfo.name, key, avroBytesWithOptionalPreamble)

    try {
      metricsContext.increment("published_records")
      kafkaProducer.send(
        record,
        (metadata: RecordMetadata, exception: Exception) => {
          if (exception != null) {
            metricsContext.increment("publish_failures")
            logger.warn(s"Failed to publish feature response to Kafka topic ${topicInfo.name} for join $key", exception)
          } else {
            metricsContext.increment("publish_successes")
          }
        }
      )
    } catch {
      case e: Exception =>
        metricsContext.increment("publish_failures")
        logger.warn(s"Failed to publish feature response to Kafka topic ${topicInfo.name} for join $key", e)
    }
  }
}
