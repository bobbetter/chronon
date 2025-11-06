package ai.chronon.flink.test.deser

import ai.chronon.flink.deser.SchemaRegistrySerDe
import ai.chronon.flink.deser.SchemaRegistrySerDe.RegistryHostKey
import ai.chronon.online.TopicInfo
import io.confluent.kafka.schemaregistry.SchemaProvider
import io.confluent.kafka.schemaregistry.avro.{AvroSchema, AvroSchemaProvider}
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.protobuf.{ProtobufSchema, ProtobufSchemaProvider}
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

class MockSchemaRegistrySerDe(topicInfo: TopicInfo, mockSchemaRegistryClient: MockSchemaRegistryClient)
    extends SchemaRegistrySerDe(topicInfo) {
  override def buildSchemaRegistryClient(schemeString: String,
                                         registryHost: String,
                                         maybePortString: Option[String]): MockSchemaRegistryClient =
    mockSchemaRegistryClient
}

class SchemaRegistrySerDeSpec extends AnyFlatSpec {

  private val avroSchemaProvider: SchemaProvider = new AvroSchemaProvider
  private val protoSchemaProvider: SchemaProvider = new ProtobufSchemaProvider
  val schemaRegistryClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider, protoSchemaProvider).asJava)

  it should "fail if the schema subject is not found" in {
    val topicInfo = TopicInfo("test-topic-avro", "kafka", Map(RegistryHostKey -> "localhost"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)
    assertThrows[IllegalArgumentException] {
      schemaRegistrySchemaProvider.schema
    }
  }

  it should "succeed if we look up an avro schema that is present" in {
    val topicInfo = TopicInfo("test-topic-avro", "kafka", Map(RegistryHostKey -> "localhost"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("test-topic-avro-value", new AvroSchema(avroSchemaStr))
    val deserSchema = schemaRegistrySchemaProvider.schema
    assert(deserSchema != null)
  }

  it should "succeed if we look up an avro schema using injected subject" in {
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("my-subject", new AvroSchema(avroSchemaStr))

    val topicInfo = TopicInfo("another-topic", "kafka", Map(RegistryHostKey -> "localhost", "subject" -> "my-subject"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val deserSchema = schemaRegistrySchemaProvider.schema
    assert(deserSchema != null)
  }

  it should "fail if we're trying to retrieve a proto schema" in {
    val protoSchemaStr = "message Foo { required string f" + " = 1; }"
    schemaRegistryClient.register("test-topic-proto-value", new ProtobufSchema(protoSchemaStr))

    val topicInfo = TopicInfo("test-topic-proto", "kafka", Map(RegistryHostKey -> "localhost"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    assertThrows[IllegalArgumentException] {
      schemaRegistrySchemaProvider.schema
    }
  }
}
