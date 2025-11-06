package ai.chronon.flink.deser

import ai.chronon.api.StructType
import ai.chronon.flink.FlinkKafkaItemEventDriver
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroConversions, AvroSerDe, Mutation, SerDe}

// Configured in topic config in this fashion:
// kafka://my-test-topic/provider_class=ai.chronon.flink.deser.MockCustomSchemaProvider/schema_name=item_event
object CustomSchemaSerDe {
  val ProviderClass = "provider_class"
  val SchemaName = "schema_name"
}

/** Mock custom schema provider that vends out a custom hardcoded event schema
  */
class MockCustomSchemaProvider(topicInfo: TopicInfo) extends SerDe {
  private val schemaName = topicInfo.params.getOrElse(CustomSchemaSerDe.SchemaName, "item_event")
  require(schemaName == "item_event", s"Schema name must be 'item_event', but got $schemaName")

  lazy val chrononSchema: StructType =
    AvroConversions.toChrononSchema(FlinkKafkaItemEventDriver.avroSchema).asInstanceOf[StructType]

  lazy val avroSerDe = new AvroSerDe(FlinkKafkaItemEventDriver.avroSchema)

  override def schema: StructType = chrononSchema

  override def fromBytes(messageBytes: Array[Byte]): Mutation = {
    avroSerDe.fromBytes(messageBytes)
  }
}
