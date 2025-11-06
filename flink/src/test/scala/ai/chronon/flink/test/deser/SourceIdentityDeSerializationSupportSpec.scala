package ai.chronon.flink.test.deser

import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.flink.deser.DeserializationSchemaBuilder
import ai.chronon.flink.test.UserAvroSchema
import ai.chronon.online.serde.{AvroConversions, SparkConversions}
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.spark.sql.Row
import org.junit.Assert.assertTrue
import org.scalatest.flatspec.AnyFlatSpec

import java.util

class SourceIdentityDeSerializationSupportSpec extends AnyFlatSpec {
  import AvroObjectCreator._

  it should "deserialize avro data" in {
    val schemaStr = UserAvroSchema.schema.toString(true)
    val groupBy = makeMetadataOnlyGroupBy()
    val avroSerdeProvider = new InMemoryAvroDeserializationSchemaProvider(UserAvroSchema.schema)
    val deserSchema = DeserializationSchemaBuilder.buildSourceIdentityDeserSchema(avroSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val resultList = new util.ArrayList[Row]()
    val listCollector = new ListCollector(resultList)

    val recordBytes = createDummyRecordBytes(schemaStr)
    deserSchema.deserialize(recordBytes, listCollector)

    // check we got a row back
    assert(resultList.size() == 1)
    val row = resultList.toScala.head

    // sanity check source event schemas line up between the deSerSchema and the original schema
    val schema = deserSchema.sourceEventEncoder.schema
    val avroSchemaFields = UserAvroSchema.schema.getFields.toScala
    val avroSchemaToSparkDataTypes = avroSchemaFields.map { field =>
      val avroType = SparkConversions.fromChrononType(AvroConversions.toChrononSchema(field.schema()))
      field.name() -> avroType
    }.toMap

    assert(schema.fieldNames sameElements avroSchemaFields.map(_.name()))
    schema.fieldNames.foreach(name => assert(schema(name).dataType == avroSchemaToSparkDataTypes(name)))
    // spot check a couple of fields
    assert(row.get(0) == 12345)
    assert(row.getString(1) == "johndoe")
  }

  it should "skip avro data that can't be deserialized" in {
    val schemaStr = UserAvroSchema.schema.toString(true)
    val groupBy = makeMetadataOnlyGroupBy()

    val avroSerdeProvider = new InMemoryAvroDeserializationSchemaProvider(UserAvroSchema.schema)
    val deserSchema = DeserializationSchemaBuilder.buildSourceIdentityDeserSchema(avroSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = AvroObjectCreator.createDummyRecordBytes(schemaStr)
    // corrupt the record bytes
    recordBytes(0) = 0

    val resultList = new util.ArrayList[Row]()
    val listCollector = new ListCollector(resultList)

    deserSchema.deserialize(recordBytes, listCollector)
    assertTrue(resultList.isEmpty)
  }
}
