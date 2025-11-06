package ai.chronon.online.test

import ai.chronon.api.ScalaJavaConversions.ListOps
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.flatspec.AnyFlatSpec

import java.io.ByteArrayOutputStream
import java.util
import scala.util.{Failure, Success, Try}

// Tests backward (new schema can read data written with old schema) and forward compatibility
// (old schema can read data written with new schema) of Avro schemas generated using old & new Chronon AvroConversions
class AvroCompatibilityTest extends AnyFlatSpec {
  import AvroCompatibilityTest._

  private def serializeRecord(record: GenericRecord, schema: Schema): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    val writer = new GenericDatumWriter[GenericRecord](schema)
    writer.write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

  private def deserializeRecord(data: Array[Byte], readerSchema: Schema): Try[GenericRecord] = {
    Try {
      val decoder = DecoderFactory.get().binaryDecoder(data, null)
      val reader = new GenericDatumReader[GenericRecord](readerSchema)
      reader.read(null, decoder)
    }
  }

  private def validateRecord(record: GenericRecord): Unit = {
    val collapsedIr = record.get("collapsedIr").asInstanceOf[GenericRecord]
    val doubleValSum = collapsedIr.get("double_val_sum_1d")
    assert(doubleValSum != null, "double_val_sum_1d should not be null")

    // Try to access different array fields that might exist
    List("named_struct_last2_1d",
         "named_struct_last2_2d",
         "another_named_struct_last2_1d",
         "another_named_struct_last2_2d").foreach { fieldName =>
      val array = collapsedIr.get(fieldName).asInstanceOf[java.util.List[GenericRecord]]
      if (array != null && !array.isEmpty) {
        val firstElement = array.get(0)
        val schemaName = firstElement.getSchema.getName
        assert(schemaName != null, s"Schema name for $fieldName should not be null")
        // Validate that we can access all fields
        firstElement.getSchema.getFields.toScala.foreach { field =>
          firstElement.get(field.name())
        }
      }
    }

    // Try int_val_last_1d
    val intValLast = collapsedIr.get("int_val_last_1d").asInstanceOf[GenericRecord]
    if (intValLast != null) {
      val schemaName = intValLast.getSchema.getName
      assert(schemaName != null, "Schema name for int_val_last_1d should not be null")
      intValLast.getSchema.getFields.toScala.foreach { field =>
        intValLast.get(field.name())
      }
    }
  }

  "Namespace approach" should "successfully deserialize records written with old schema" in {
    val oldSchema = buildOldSchema()
    val oldRecord = createTestRecord(oldSchema)
    val serializedOld = serializeRecord(oldRecord, oldSchema)

    val namespaceReaderSchema = buildNamespaceApproachSchema()
    val deserializedWithNamespace = deserializeRecord(serializedOld, namespaceReaderSchema)

    deserializedWithNamespace match {
      case Success(record) =>
        validateRecord(record)
        succeed
      case Failure(exception) =>
        fail(s"Namespace approach failed: ${exception.getMessage}")
    }
  }

  "Field name approach" should "successfully deserialize records written with old schema" in {
    val oldSchema = buildOldSchema()
    val oldRecord = createTestRecord(oldSchema)
    val serializedOld = serializeRecord(oldRecord, oldSchema)

    val fieldNameReaderSchema = buildFieldNameApproachSchema()
    val deserializedWithFieldName = deserializeRecord(serializedOld, fieldNameReaderSchema)

    deserializedWithFieldName match {
      case Success(record) =>
        validateRecord(record)
        succeed
      case Failure(exception) =>
        fail(s"Field name approach failed: ${exception.getMessage}")
    }
  }

  "Old schema" should "successfully deserialize records written with namespace approach schema" in {
    val namespaceSchema = buildNamespaceApproachSchema()
    val namespaceRecord = createTestRecord(namespaceSchema)
    val serializedNamespace = serializeRecord(namespaceRecord, namespaceSchema)

    val oldReaderSchema = buildOldSchema()
    val deserializedWithOld = deserializeRecord(serializedNamespace, oldReaderSchema)

    deserializedWithOld match {
      case Success(record) =>
        validateRecord(record)
        succeed
      case Failure(exception) =>
        fail(s"Old schema reading namespace approach failed: ${exception.getMessage}")
    }
  }

  "Old schema" should "successfully deserialize records written with field name approach schema" in {
    val fieldNameSchema = buildFieldNameApproachSchema()
    val fieldNameRecord = createTestRecord(fieldNameSchema)
    val serializedFieldName = serializeRecord(fieldNameRecord, fieldNameSchema)

    val oldReaderSchema = buildOldSchema()
    val deserializedWithOld = deserializeRecord(serializedFieldName, oldReaderSchema)

    deserializedWithOld match {
      case Success(record) =>
        validateRecord(record)
        succeed
      case Failure(exception) =>
        fail(s"Old schema reading field name approach failed: ${exception.getMessage}")
    }
  }
}

private object AvroCompatibilityTest {

  // Build schema in the old Chronon format with repeated names
  private def buildOldSchema(): Schema = {
    // PayloadStruct (clean, used in first field)
    val payloadStructClean = SchemaBuilder
      .record("PayloadStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("id")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .endUnion()
      .noDefault()
      .name("int_val")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .endRecord()

    // PayloadStruct_REPEATED_NAME_0 (used in second field)
    val payloadStruct0 = SchemaBuilder
      .record("PayloadStruct_REPEATED_NAME_0")
      .namespace("ai.chronon.data")
      .fields()
      .name("id_REPEATED_NAME_0")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .endUnion()
      .noDefault()
      .name("int_val_REPEATED_NAME_0")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .endRecord()

    // PayloadStruct_REPEATED_NAME_1 (used in third field, with double_val instead of int_val)
    val payloadStruct1 = SchemaBuilder
      .record("PayloadStruct_REPEATED_NAME_1")
      .namespace("ai.chronon.data")
      .fields()
      .name("id_REPEATED_NAME_1")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .endUnion()
      .noDefault()
      .name("double_val")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .doubleType()
      .endUnion()
      .noDefault()
      .endRecord()

    // PayloadStruct_REPEATED_NAME_2 (used in fourth field)
    val payloadStruct2 = SchemaBuilder
      .record("PayloadStruct_REPEATED_NAME_2")
      .namespace("ai.chronon.data")
      .fields()
      .name("id_REPEATED_NAME_2")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .endUnion()
      .noDefault()
      .name("double_val_REPEATED_NAME_0")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .doubleType()
      .endUnion()
      .noDefault()
      .endRecord()

    // Element structures
    val element1d = SchemaBuilder
      .record("Named_struct_last2_1dElementStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("epochMillis")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStructClean)
      .endUnion()
      .noDefault()
      .endRecord()

    val element2d = SchemaBuilder
      .record("Named_struct_last2_2dElementStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("epochMillis_REPEATED_NAME_0")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload_REPEATED_NAME_0")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStruct0)
      .endUnion()
      .noDefault()
      .endRecord()

    val anotherElement1d = SchemaBuilder
      .record("Another_named_struct_last2_1dElementStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("epochMillis_REPEATED_NAME_1")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload_REPEATED_NAME_1")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStruct1)
      .endUnion()
      .noDefault()
      .endRecord()

    val anotherElement2d = SchemaBuilder
      .record("Another_named_struct_last2_2dElementStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("epochMillis_REPEATED_NAME_2")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload_REPEATED_NAME_2")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStruct2)
      .endUnion()
      .noDefault()
      .endRecord()

    val intValLastStruct = SchemaBuilder
      .record("Int_val_last_1dStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("epochMillis_REPEATED_NAME_3")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload_REPEATED_NAME_3")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .endRecord()

    // CollapsedIr structure
    val collapsedIrSchema = SchemaBuilder
      .record("CollapsedIrStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("double_val_sum_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .doubleType()
      .endUnion()
      .noDefault()
      .name("named_struct_last2_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(element1d)
      .endUnion()
      .noDefault()
      .name("named_struct_last2_2d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(element2d)
      .endUnion()
      .noDefault()
      .name("another_named_struct_last2_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(anotherElement1d)
      .endUnion()
      .noDefault()
      .name("another_named_struct_last2_2d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(anotherElement2d)
      .endUnion()
      .noDefault()
      .name("int_val_last_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(intValLastStruct)
      .endUnion()
      .noDefault()
      .endRecord()

    // Root Value record
    SchemaBuilder
      .record("Value")
      .namespace("ai.chronon.data")
      .fields()
      .name("collapsedIr")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(collapsedIrSchema)
      .endUnion()
      .noDefault()
      .endRecord()
  }

  // Build a schema using the namespace based field paths
  private def buildNamespaceApproachSchema(): Schema = {
    val payloadStruct = SchemaBuilder
      .record("PayloadStruct")
      .namespace("ai.chronon.data.collapsedIr.named_struct_last2_1d.payload")
      .fields()
      .name("id")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .endUnion()
      .noDefault()
      .name("int_val")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .endRecord()

    val payloadStructWithDouble = SchemaBuilder
      .record("PayloadStruct")
      .namespace("ai.chronon.data.collapsedIr.another_named_struct_last2_1d.payload")
      .fields()
      .name("id")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .endUnion()
      .noDefault()
      .name("double_val")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .doubleType()
      .endUnion()
      .noDefault()
      .endRecord()

    val element1d = SchemaBuilder
      .record("Named_struct_last2_1dElementStruct")
      .namespace("ai.chronon.data.collapsedIr.named_struct_last2_1d")
      .fields()
      .name("epochMillis")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStruct)
      .endUnion()
      .noDefault()
      .endRecord()

    val payloadStruct2d = SchemaBuilder
      .record("PayloadStruct")
      .namespace("ai.chronon.data.collapsedIr.named_struct_last2_2d.payload")
      .fields()
      .name("id")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .endUnion()
      .noDefault()
      .name("int_val")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .endRecord()

    val element2d = SchemaBuilder
      .record("Named_struct_last2_2dElementStruct")
      .namespace("ai.chronon.data.collapsedIr.named_struct_last2_2d")
      .fields()
      .name("epochMillis")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStruct2d)
      .endUnion()
      .noDefault()
      .endRecord()

    val anotherElement1d = SchemaBuilder
      .record("Another_named_struct_last2_1dElementStruct")
      .namespace("ai.chronon.data.collapsedIr.another_named_struct_last2_1d")
      .fields()
      .name("epochMillis")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStructWithDouble)
      .endUnion()
      .noDefault()
      .endRecord()

    val payloadStructAnother2d = SchemaBuilder
      .record("PayloadStruct")
      .namespace("ai.chronon.data.collapsedIr.another_named_struct_last2_2d.payload")
      .fields()
      .name("id")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .endUnion()
      .noDefault()
      .name("double_val")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .doubleType()
      .endUnion()
      .noDefault()
      .endRecord()

    val anotherElement2d = SchemaBuilder
      .record("Another_named_struct_last2_2dElementStruct")
      .namespace("ai.chronon.data.collapsedIr.another_named_struct_last2_2d")
      .fields()
      .name("epochMillis")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStructAnother2d)
      .endUnion()
      .noDefault()
      .endRecord()

    val intValLastStruct = SchemaBuilder
      .record("Int_val_last_1dStruct")
      .namespace("ai.chronon.data.collapsedIr.int_val_last_1d")
      .fields()
      .name("epochMillis")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .endRecord()

    val collapsedIrSchema = SchemaBuilder
      .record("CollapsedIrStruct")
      .namespace("ai.chronon.data.collapsedIr")
      .fields()
      .name("double_val_sum_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .doubleType()
      .endUnion()
      .noDefault()
      .name("named_struct_last2_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(element1d)
      .endUnion()
      .noDefault()
      .name("named_struct_last2_2d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(element2d)
      .endUnion()
      .noDefault()
      .name("another_named_struct_last2_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(anotherElement1d)
      .endUnion()
      .noDefault()
      .name("another_named_struct_last2_2d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(anotherElement2d)
      .endUnion()
      .noDefault()
      .name("int_val_last_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(intValLastStruct)
      .endUnion()
      .noDefault()
      .endRecord()

    SchemaBuilder
      .record("Value")
      .namespace("ai.chronon.data")
      .fields()
      .name("collapsedIr")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(collapsedIrSchema)
      .endUnion()
      .noDefault()
      .endRecord()
  }

  private def buildFieldNameApproachSchema(): Schema = {
    val payloadStruct = SchemaBuilder
      .record("collapsedIr_named_struct_last2_1d_payload_PayloadStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("id")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .endUnion()
      .noDefault()
      .name("int_val")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .endRecord()

    val payloadStructWithDouble = SchemaBuilder
      .record("collapsedIr_another_named_struct_last2_1d_payload_PayloadStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("id")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .endUnion()
      .noDefault()
      .name("double_val")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .doubleType()
      .endUnion()
      .noDefault()
      .endRecord()

    val element1d = SchemaBuilder
      .record("collapsedIr_named_struct_last2_1d_Named_struct_last2_1dElementStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("epochMillis")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStruct)
      .endUnion()
      .noDefault()
      .endRecord()

    val element2d = SchemaBuilder
      .record("collapsedIr_named_struct_last2_2d_Named_struct_last2_2dElementStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("epochMillis")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStruct)
      .endUnion()
      .noDefault()
      .endRecord()

    val anotherElement1d = SchemaBuilder
      .record("collapsedIr_another_named_struct_last2_1d_Another_named_struct_last2_1dElementStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("epochMillis")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStructWithDouble)
      .endUnion()
      .noDefault()
      .endRecord()

    val anotherElement2d = SchemaBuilder
      .record("collapsedIr_another_named_struct_last2_2d_Another_named_struct_last2_2dElementStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("epochMillis")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(payloadStructWithDouble)
      .endUnion()
      .noDefault()
      .endRecord()

    val intValLastStruct = SchemaBuilder
      .record("collapsedIr_int_val_last_1d_Int_val_last_1dStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("epochMillis")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("payload")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .endRecord()

    val collapsedIrSchema = SchemaBuilder
      .record("collapsedIr_CollapsedIrStruct")
      .namespace("ai.chronon.data")
      .fields()
      .name("double_val_sum_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .doubleType()
      .endUnion()
      .noDefault()
      .name("named_struct_last2_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(element1d)
      .endUnion()
      .noDefault()
      .name("named_struct_last2_2d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(element2d)
      .endUnion()
      .noDefault()
      .name("another_named_struct_last2_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(anotherElement1d)
      .endUnion()
      .noDefault()
      .name("another_named_struct_last2_2d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .array()
      .items(anotherElement2d)
      .endUnion()
      .noDefault()
      .name("int_val_last_1d")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(intValLastStruct)
      .endUnion()
      .noDefault()
      .endRecord()

    SchemaBuilder
      .record("Value")
      .namespace("ai.chronon.data")
      .fields()
      .name("collapsedIr")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .`type`(collapsedIrSchema)
      .endUnion()
      .noDefault()
      .endRecord()
  }

  private def createTestRecord(schema: Schema): GenericRecord = {
    // Find the collapsedIr schema
    val valueField = schema.getField("collapsedIr")
    val collapsedIrSchema = valueField.schema().getTypes.toScala.find(_.getType == Schema.Type.RECORD).get

    // Create various payload records based on what's expected in the schema
    val payloadFields = findAllPayloadSchemas(collapsedIrSchema)

    payloadFields.foreach { payloadSchema =>
      val record = new GenericData.Record(payloadSchema)
      // Set fields based on what exists in this particular payload schema
      payloadSchema.getFields.toScala.foreach { field =>
        field.name() match {
          case name if name.contains("id")         => record.put(name, "test_user_123")
          case name if name.contains("int_val")    => record.put(name, 42)
          case name if name.contains("double_val") => record.put(name, 123.45)
          case _                                   => // ignore other fields
        }
      }
      record
    }

    // Create element records for arrays
    val elementSchemas = findAllElementSchemas(collapsedIrSchema)
    elementSchemas.foreach { elementSchema =>
      val element = new GenericData.Record(elementSchema)
      elementSchema.getFields.toScala.foreach { field =>
        field.name() match {
          case name if name.contains("epochMillis") => element.put(name, 1640995200000L)
          case name if name.contains("payload") =>
            val payloadType = field.schema().getTypes.toScala.find(_.getType == Schema.Type.RECORD).get
            val payload = new GenericData.Record(payloadType)
            payloadType.getFields.toScala.foreach { pField =>
              pField.name() match {
                case pName if pName.contains("id")         => payload.put(pName, "user123")
                case pName if pName.contains("int_val")    => payload.put(pName, 42)
                case pName if pName.contains("double_val") => payload.put(pName, 123.45)
                case _                                     => // ignore
              }
            }
            element.put(name, payload)
          case _ => // ignore
        }
      }
      element
    }

    // Create collapsedIr record
    val collapsedIr = new GenericData.Record(collapsedIrSchema)
    collapsedIr.put("double_val_sum_1d", 999.99)

    // Add arrays where they exist
    collapsedIrSchema.getFields.toScala.foreach { field =>
      field.name() match {
        case "named_struct_last2_1d" | "named_struct_last2_2d" | "another_named_struct_last2_1d" |
            "another_named_struct_last2_2d" =>
          val arrayType = field.schema().getTypes.toScala.find(_.getType == Schema.Type.ARRAY).get
          val elementType = arrayType.getElementType
          val element = new GenericData.Record(elementType)

          // Fill the element based on its schema
          elementType.getFields.toScala.foreach { eField =>
            eField.name() match {
              case eName if eName.contains("epochMillis") => element.put(eName, 1640995200000L)
              case eName if eName.contains("payload") =>
                val pType = eField.schema().getTypes.toScala.find(_.getType == Schema.Type.RECORD).get
                val payload = new GenericData.Record(pType)
                pType.getFields.toScala.foreach { pField =>
                  pField.name() match {
                    case pName if pName.contains("id")         => payload.put(pName, "test123")
                    case pName if pName.contains("int_val")    => payload.put(pName, 42)
                    case pName if pName.contains("double_val") => payload.put(pName, 123.45)
                    case _                                     => // ignore
                  }
                }
                element.put(eName, payload)
              case _ => // ignore
            }
          }
          collapsedIr.put(field.name(), util.Arrays.asList(element))
        case "int_val_last_1d" =>
          val recordType = field.schema().getTypes.toScala.find(_.getType == Schema.Type.RECORD).get
          val intValRecord = new GenericData.Record(recordType)
          recordType.getFields.toScala.foreach { iField =>
            iField.name() match {
              case iName if iName.contains("epochMillis") => intValRecord.put(iName, 1640995200000L)
              case iName if iName.contains("payload")     => intValRecord.put(iName, 789)
              case _                                      => // ignore
            }
          }
          collapsedIr.put(field.name(), intValRecord)
        case _ => // already handled above
      }
    }

    // Create root record
    val rootRecord = new GenericData.Record(schema)
    rootRecord.put("collapsedIr", collapsedIr)
    rootRecord
  }

  private def findAllPayloadSchemas(schema: Schema): List[Schema] = {
    def search(s: Schema): List[Schema] = s.getType match {
      case Schema.Type.RECORD =>
        val current = if (s.getName.contains("PayloadStruct")) List(s) else List.empty
        val nested = s.getFields.toScala.flatMap(f => search(f.schema())).toList
        current ++ nested
      case Schema.Type.ARRAY => search(s.getElementType)
      case Schema.Type.UNION => s.getTypes.toScala.flatMap(search).toList
      case _                 => List.empty
    }
    search(schema)
  }

  private def findAllElementSchemas(schema: Schema): List[Schema] = {
    def search(s: Schema): List[Schema] = s.getType match {
      case Schema.Type.RECORD =>
        val current = if (s.getName.contains("ElementStruct")) List(s) else List.empty
        val nested = s.getFields.toScala.flatMap(f => search(f.schema())).toList
        current ++ nested
      case Schema.Type.ARRAY => search(s.getElementType)
      case Schema.Type.UNION => s.getTypes.toScala.flatMap(search).toList
      case _                 => List.empty
    }
    search(schema)
  }

}
