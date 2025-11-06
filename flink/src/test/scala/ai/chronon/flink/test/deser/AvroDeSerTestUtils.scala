package ai.chronon.flink.test.deser

import ai.chronon.api.{Accuracy, Builders, GroupBy, StructType}
import ai.chronon.online.serde.{AvroCodec, AvroSerDe, Mutation, SerDe}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.util.{SimpleUserCodeClassLoader, UserCodeClassLoader}

import scala.collection.JavaConverters._

class DummyInitializationContext
    extends SerializationSchema.InitializationContext
    with DeserializationSchema.InitializationContext {
  override def getMetricGroup = new UnregisteredMetricsGroup

  override def getUserCodeClassLoader: UserCodeClassLoader =
    SimpleUserCodeClassLoader.create(classOf[DummyInitializationContext].getClassLoader)
}

class InMemoryAvroDeserializationSchemaProvider(schema: Schema) extends SerDe {
  val avroSerDe = new AvroSerDe(schema)
  override def schema: StructType = avroSerDe.schema
  override def fromBytes(message: Array[Byte]): Mutation = {
    avroSerDe.fromBytes(message)
  }
}

object AvroObjectCreator {
  def createDummyRecordBytes(schemaStr: String): Array[Byte] = {
    // Create the main record
    val avroCodec = AvroCodec.of(schemaStr)
    val schema = avroCodec.schema
    val record = new GenericData.Record(schema)

    // Create the nested address record
    val addressSchema = schema.getField("address").schema()
    val address = new GenericData.Record(addressSchema)
    address.put("street", "123 Main St")
    address.put("city", "San Francisco")
    address.put("country", "USA")
    address.put("postalCode", "94105")

    // Create an array of tags
    val tags = new GenericData.Array[String](
      schema.getField("tags").schema(),
      List("active", "premium", "verified").asJava
    )

    // Create a map of preferences
    val preferences = Map(
      "theme" -> "dark",
      "notifications" -> "enabled",
      "language" -> "en"
    ).asJava

    // Fill in all the fields
    record.put("id", 12345)
    record.put("username", "johndoe")
    record.put("tags", tags)
    record.put("address", address)
    record.put("preferences", preferences)
    record.put("lastLoginTimestamp", System.currentTimeMillis())
    record.put("isActive", true)

    avroCodec.encodeBinary(record)
  }

  def makeMetadataOnlyGroupBy(): GroupBy = {
    // this can be a thin GroupBy as we don't need to run any actual operations
    Builders.GroupBy(
      sources = Seq.empty,
      metaData = Builders.MetaData(
        name = "user-count"
      ),
      accuracy = Accuracy.TEMPORAL
    )
  }

  def makeGroupBy(projections: Map[String, String], filters: Seq[String] = Seq.empty): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.my_stream_raw",
          topic = "events.my_stream",
          query = Builders.Query(
            selects = projections,
            wheres = filters,
            timeColumn = "lastLoginTimestamp",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = Seq("username"),
      aggregations = Seq.empty,
      metaData = Builders.MetaData(
        name = "user-groupby"
      ),
      accuracy = Accuracy.TEMPORAL
    )
}
