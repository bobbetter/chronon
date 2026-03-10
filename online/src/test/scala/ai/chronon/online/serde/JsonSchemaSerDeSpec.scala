package ai.chronon.online.serde

import ai.chronon.api._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class JsonSchemaSerDeSpec extends AnyFlatSpec with Matchers {

  private val flatSchema =
    """{
      |  "title": "user_event",
      |  "type": "object",
      |  "properties": {
      |    "user_id": { "type": "string" },
      |    "ts": { "type": "integer" },
      |    "score": { "type": "number" },
      |    "active": { "type": "boolean" }
      |  }
      |}""".stripMargin

  // --- Schema Parsing ---

  it should "parse flat JSON Schema with primitive types" in {
    val serDe = new JsonSchemaSerDe(flatSchema, "user_event")
    val schema = serDe.schema

    schema.name shouldBe "user_event"
    schema.fields.length shouldBe 4
    schema.fields.find(_.name == "user_id").get.fieldType shouldBe StringType
    schema.fields.find(_.name == "ts").get.fieldType shouldBe LongType
    schema.fields.find(_.name == "score").get.fieldType shouldBe DoubleType
    schema.fields.find(_.name == "active").get.fieldType shouldBe BooleanType
  }

  it should "use schemaName when title is absent" in {
    val schemaNoTitle =
      """{
        |  "type": "object",
        |  "properties": {
        |    "x": { "type": "string" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schemaNoTitle, "fallback_name")
    serDe.schema.name shouldBe "fallback_name"
  }

  it should "parse array types" in {
    val schema =
      """{
        |  "title": "list_test",
        |  "type": "object",
        |  "properties": {
        |    "tags": { "type": "array", "items": { "type": "string" } },
        |    "scores": { "type": "array", "items": { "type": "number" } }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "list_test")
    val parsed = serDe.schema

    parsed.fields.find(_.name == "tags").get.fieldType shouldBe ListType(StringType)
    parsed.fields.find(_.name == "scores").get.fieldType shouldBe ListType(DoubleType)
  }

  it should "parse map types via additionalProperties" in {
    val schema =
      """{
        |  "title": "map_test",
        |  "type": "object",
        |  "properties": {
        |    "metadata": {
        |      "type": "object",
        |      "additionalProperties": { "type": "string" }
        |    },
        |    "counts": {
        |      "type": "object",
        |      "additionalProperties": { "type": "integer" }
        |    }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "map_test")
    val parsed = serDe.schema

    parsed.fields.find(_.name == "metadata").get.fieldType shouldBe MapType(StringType, StringType)
    parsed.fields.find(_.name == "counts").get.fieldType shouldBe MapType(StringType, LongType)
  }

  it should "parse nested struct types" in {
    val schema =
      """{
        |  "title": "nested_test",
        |  "type": "object",
        |  "properties": {
        |    "name": { "type": "string" },
        |    "address": {
        |      "type": "object",
        |      "title": "address",
        |      "properties": {
        |        "street": { "type": "string" },
        |        "zip": { "type": "string" },
        |        "floor": { "type": "integer" }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "nested_test")
    val parsed = serDe.schema

    parsed.fields.length shouldBe 2
    val addressType = parsed.fields.find(_.name == "address").get.fieldType
    addressType shouldBe a[StructType]
    val addressStruct = addressType.asInstanceOf[StructType]
    addressStruct.fields.length shouldBe 3
    addressStruct.fields.find(_.name == "street").get.fieldType shouldBe StringType
    addressStruct.fields.find(_.name == "zip").get.fieldType shouldBe StringType
    addressStruct.fields.find(_.name == "floor").get.fieldType shouldBe LongType
  }

  it should "default object without properties or additionalProperties to MapType(StringType, StringType)" in {
    val schema =
      """{
        |  "title": "bare_object",
        |  "type": "object",
        |  "properties": {
        |    "data": { "type": "object" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "bare_object")
    serDe.schema.fields.find(_.name == "data").get.fieldType shouldBe MapType(StringType, StringType)
  }

  it should "default array items to StringType when items is absent" in {
    val schema =
      """{
        |  "title": "no_items",
        |  "type": "object",
        |  "properties": {
        |    "values": { "type": "array" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "no_items")
    serDe.schema.fields.find(_.name == "values").get.fieldType shouldBe ListType(StringType)
  }

  // --- Nullable type arrays ---

  it should "parse nullable string type [\"string\", \"null\"]" in {
    val schema =
      """{
        |  "title": "nullable_test",
        |  "type": "object",
        |  "properties": {
        |    "name": { "type": ["string", "null"] }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "nullable_test")
    serDe.schema.fields.find(_.name == "name").get.fieldType shouldBe StringType
  }

  it should "parse nullable integer type [\"integer\", \"null\"]" in {
    val schema =
      """{
        |  "title": "nullable_test",
        |  "type": "object",
        |  "properties": {
        |    "count": { "type": ["integer", "null"] }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "nullable_test")
    serDe.schema.fields.find(_.name == "count").get.fieldType shouldBe LongType
  }

  it should "parse nullable number type [\"number\", \"null\"]" in {
    val schema =
      """{
        |  "title": "nullable_test",
        |  "type": "object",
        |  "properties": {
        |    "ratio": { "type": ["number", "null"] }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "nullable_test")
    serDe.schema.fields.find(_.name == "ratio").get.fieldType shouldBe DoubleType
  }

  it should "parse nullable boolean type [\"boolean\", \"null\"]" in {
    val schema =
      """{
        |  "title": "nullable_test",
        |  "type": "object",
        |  "properties": {
        |    "flag": { "type": ["boolean", "null"] }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "nullable_test")
    serDe.schema.fields.find(_.name == "flag").get.fieldType shouldBe BooleanType
  }

  it should "parse nullable array type [\"array\", \"null\"] with items" in {
    val schema =
      """{
        |  "title": "nullable_test",
        |  "type": "object",
        |  "properties": {
        |    "tags": { "type": ["array", "null"], "items": { "type": "string" } }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "nullable_test")
    serDe.schema.fields.find(_.name == "tags").get.fieldType shouldBe ListType(StringType)
  }

  // --- Format annotations ---

  it should "parse date format as DateType" in {
    val schema =
      """{
        |  "title": "format_test",
        |  "type": "object",
        |  "properties": {
        |    "birth_date": { "type": "string", "format": "date" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "format_test")
    serDe.schema.fields.find(_.name == "birth_date").get.fieldType shouldBe DateType
  }

  it should "parse date-time format as TimestampType" in {
    val schema =
      """{
        |  "title": "format_test",
        |  "type": "object",
        |  "properties": {
        |    "created_at": { "type": "string", "format": "date-time" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "format_test")
    serDe.schema.fields.find(_.name == "created_at").get.fieldType shouldBe TimestampType
  }

  it should "parse base64 contentEncoding as BinaryType" in {
    val schema =
      """{
        |  "title": "format_test",
        |  "type": "object",
        |  "properties": {
        |    "payload": { "type": "string", "contentEncoding": "base64" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "format_test")
    serDe.schema.fields.find(_.name == "payload").get.fieldType shouldBe BinaryType
  }

  it should "parse number with decimal format as DecimalType" in {
    val schema =
      """{
        |  "title": "format_test",
        |  "type": "object",
        |  "properties": {
        |    "amount": { "type": "number", "format": "decimal", "precision": 10, "scale": 2 }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "format_test")
    serDe.schema.fields.find(_.name == "amount").get.fieldType shouldBe DecimalType(10, 2)
  }

  it should "still parse plain string as StringType (regression)" in {
    val schema =
      """{
        |  "title": "regression_test",
        |  "type": "object",
        |  "properties": {
        |    "name": { "type": "string" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "regression_test")
    serDe.schema.fields.find(_.name == "name").get.fieldType shouldBe StringType
  }

  it should "still parse plain number as DoubleType (regression)" in {
    val schema =
      """{
        |  "title": "regression_test",
        |  "type": "object",
        |  "properties": {
        |    "ratio": { "type": "number" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "regression_test")
    serDe.schema.fields.find(_.name == "ratio").get.fieldType shouldBe DoubleType
  }

  // --- Mutation/reversal support ---

  it should "produce before-populated Mutation when is_before is true" in {
    val reversalSchema =
      """{
        |  "title": "reversal_test",
        |  "type": "object",
        |  "properties": {
        |    "user_id": { "type": "string" },
        |    "is_before": { "type": "boolean" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(reversalSchema, "reversal_test")
    val message = """{"user_id": "u1", "is_before": true}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    mutation.before should not be null
    mutation.after shouldBe null
  }

  it should "produce after-populated Mutation when is_before is false" in {
    val reversalSchema =
      """{
        |  "title": "reversal_test",
        |  "type": "object",
        |  "properties": {
        |    "user_id": { "type": "string" },
        |    "is_before": { "type": "boolean" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(reversalSchema, "reversal_test")
    val message = """{"user_id": "u1", "is_before": false}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    mutation.after should not be null
    mutation.before shouldBe null
  }

  it should "produce after-populated Mutation when is_before is absent from schema" in {
    val serDe = new JsonSchemaSerDe(flatSchema, "user_event")
    val message = """{"user_id": "u42", "ts": 1700000000000, "score": 0.99, "active": true}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    mutation.after should not be null
    mutation.before shouldBe null
  }

  // --- Deserialization ---

  it should "deserialize nullable fields with a value and with null" in {
    val schema =
      """{
        |  "title": "nullable_deser",
        |  "type": "object",
        |  "properties": {
        |    "count": { "type": ["integer", "null"] },
        |    "label": { "type": ["string", "null"] }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "nullable_deser")
    val s = serDe.schema

    val withValues = serDe.fromBytes("""{"count": 42, "label": "hello"}""".getBytes(StandardCharsets.UTF_8))
    withValues.after(s.indexWhere(_.name == "count")) shouldBe 42L
    withValues.after(s.indexWhere(_.name == "label")) shouldBe "hello"

    val withNulls = serDe.fromBytes("""{"count": null, "label": null}""".getBytes(StandardCharsets.UTF_8))
    assert(withNulls.after(s.indexWhere(_.name == "count")) == null)
    assert(withNulls.after(s.indexWhere(_.name == "label")) == null)
  }

  it should "deserialize date string to java.sql.Date" in {
    val schema =
      """{
        |  "title": "date_test",
        |  "type": "object",
        |  "properties": {
        |    "birth_date": { "type": "string", "format": "date" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "date_test")
    val message = """{"birth_date": "2024-01-15"}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    mutation.after(0) shouldBe java.sql.Date.valueOf("2024-01-15")
  }

  it should "deserialize date-time string to java.sql.Timestamp" in {
    val schema =
      """{
        |  "title": "ts_test",
        |  "type": "object",
        |  "properties": {
        |    "created_at": { "type": "string", "format": "date-time" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "ts_test")
    val message = """{"created_at": "2024-01-15T10:30:00Z"}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    mutation.after(0) shouldBe java.sql.Timestamp.from(java.time.Instant.parse("2024-01-15T10:30:00Z"))
  }

  it should "deserialize base64 string to Array[Byte]" in {
    val schema =
      """{
        |  "title": "binary_test",
        |  "type": "object",
        |  "properties": {
        |    "payload": { "type": "string", "contentEncoding": "base64" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "binary_test")
    val message = """{"payload": "aGVsbG8="}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    mutation.after(0).asInstanceOf[Array[Byte]] shouldBe "hello".getBytes(StandardCharsets.UTF_8)
  }

  it should "deserialize decimal string to BigDecimal" in {
    val schema =
      """{
        |  "title": "decimal_test",
        |  "type": "object",
        |  "properties": {
        |    "amount": { "type": "number", "format": "decimal", "precision": 10, "scale": 2 }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "decimal_test")
    val message = """{"amount": "123.45"}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    mutation.after(0) shouldBe new java.math.BigDecimal("123.45")
  }

  it should "deserialize flat JSON message" in {
    val serDe = new JsonSchemaSerDe(flatSchema, "user_event")
    val message = """{"user_id": "u42", "ts": 1700000000000, "score": 0.99, "active": true}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    mutation.before shouldBe null
    mutation.after should not be null

    val row = mutation.after
    val schema = serDe.schema
    row(schema.indexWhere(_.name == "user_id")) shouldBe "u42"
    row(schema.indexWhere(_.name == "ts")) shouldBe 1700000000000L
    row(schema.indexWhere(_.name == "score")) shouldBe 0.99
    row(schema.indexWhere(_.name == "active")) shouldBe java.lang.Boolean.TRUE
  }

  it should "handle null values in JSON message" in {
    val serDe = new JsonSchemaSerDe(flatSchema, "user_event")
    val message = """{"user_id": null, "ts": null, "score": null, "active": null}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    mutation.after.foreach { v => assert(v == null) }
  }

  it should "handle missing fields as null" in {
    val serDe = new JsonSchemaSerDe(flatSchema, "user_event")
    val message = """{"user_id": "u1"}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    val schema = serDe.schema
    mutation.after(schema.indexWhere(_.name == "user_id")) shouldBe "u1"
    assert(mutation.after(schema.indexWhere(_.name == "ts")) == null)
    assert(mutation.after(schema.indexWhere(_.name == "score")) == null)
    assert(mutation.after(schema.indexWhere(_.name == "active")) == null)
  }

  it should "coerce integer to long" in {
    val serDe = new JsonSchemaSerDe(flatSchema, "user_event")
    val message = """{"user_id": "u1", "ts": 42, "score": 1.0, "active": false}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    val schema = serDe.schema
    mutation.after(schema.indexWhere(_.name == "ts")) shouldBe 42L
  }

  it should "deserialize arrays" in {
    val arraySchema =
      """{
        |  "title": "arr",
        |  "type": "object",
        |  "properties": {
        |    "tags": { "type": "array", "items": { "type": "string" } }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(arraySchema, "arr")
    val message = """{"tags": ["a", "b", "c"]}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    val list = mutation.after(0).asInstanceOf[java.util.List[String]]
    list.size() shouldBe 3
    list.get(0) shouldBe "a"
    list.get(1) shouldBe "b"
    list.get(2) shouldBe "c"
  }

  it should "deserialize maps" in {
    val mapSchema =
      """{
        |  "title": "m",
        |  "type": "object",
        |  "properties": {
        |    "metadata": {
        |      "type": "object",
        |      "additionalProperties": { "type": "string" }
        |    }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(mapSchema, "m")
    val message = """{"metadata": {"key1": "val1", "key2": "val2"}}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    val map = mutation.after(0).asInstanceOf[java.util.Map[String, String]]
    map.size() shouldBe 2
    map.get("key1") shouldBe "val1"
    map.get("key2") shouldBe "val2"
  }

  it should "deserialize nested structs as Array[Any]" in {
    val nestedSchema =
      """{
        |  "title": "nested",
        |  "type": "object",
        |  "properties": {
        |    "name": { "type": "string" },
        |    "address": {
        |      "type": "object",
        |      "title": "address",
        |      "properties": {
        |        "street": { "type": "string" },
        |        "zip": { "type": "string" }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(nestedSchema, "nested")
    val message = """{"name": "Alice", "address": {"street": "123 Main St", "zip": "90210"}}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    val schema = serDe.schema
    mutation.after(schema.indexWhere(_.name == "name")) shouldBe "Alice"

    val addressRow = mutation.after(schema.indexWhere(_.name == "address")).asInstanceOf[Array[Any]]
    addressRow should not be null
    val addressType = schema.fields.find(_.name == "address").get.fieldType.asInstanceOf[StructType]
    addressRow(addressType.indexWhere(_.name == "street")) shouldBe "123 Main St"
    addressRow(addressType.indexWhere(_.name == "zip")) shouldBe "90210"
  }

  it should "coerce string numbers to numeric types" in {
    val serDe = new JsonSchemaSerDe(flatSchema, "user_event")
    val message = """{"user_id": "u1", "ts": "1700000000000", "score": "0.5", "active": "true"}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    val schema = serDe.schema
    mutation.after(schema.indexWhere(_.name == "ts")) shouldBe 1700000000000L
    mutation.after(schema.indexWhere(_.name == "score")) shouldBe 0.5
    mutation.after(schema.indexWhere(_.name == "active")) shouldBe java.lang.Boolean.TRUE
  }

  it should "convert non-string values to string when target is StringType" in {
    val stringSchema =
      """{
        |  "title": "str_test",
        |  "type": "object",
        |  "properties": {
        |    "value": { "type": "string" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(stringSchema, "str_test")

    val message = """{"value": 42}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))
    mutation.after(0) shouldBe "42"
  }

  it should "handle unicode and escaped characters in strings" in {
    val stringSchema =
      """{
        |  "title": "unicode_test",
        |  "type": "object",
        |  "properties": {
        |    "text": { "type": "string" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(stringSchema, "unicode_test")
    val message = """{"text": "hello\nworld \u00e9"}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))
    mutation.after(0) shouldBe "hello\nworld \u00e9"
  }

  // --- $ref resolution ---

  it should "parse a $ref field as the referenced struct type" in {
    val schema =
      """{
        |  "title": "event",
        |  "type": "object",
        |  "properties": {
        |    "id": { "type": "string" },
        |    "meta": { "$ref": "#/definitions/common/Meta" }
        |  },
        |  "definitions": {
        |    "common": {
        |      "Meta": {
        |        "type": "object",
        |        "title": "Meta",
        |        "properties": {
        |          "region": { "type": "string" },
        |          "count":  { "type": "integer" }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "event")
    val parsed = serDe.schema

    parsed.fields.length shouldBe 2
    val metaType = parsed.fields.find(_.name == "meta").get.fieldType
    metaType shouldBe a[StructType]
    val metaStruct = metaType.asInstanceOf[StructType]
    metaStruct.name shouldBe "Meta"
    metaStruct.fields.length shouldBe 2
    metaStruct.fields.find(_.name == "region").get.fieldType shouldBe StringType
    metaStruct.fields.find(_.name == "count").get.fieldType shouldBe LongType
  }

  it should "parse an array whose items use $ref" in {
    val schema =
      """{
        |  "title": "batch",
        |  "type": "object",
        |  "properties": {
        |    "items": {
        |      "type": "array",
        |      "items": { "$ref": "#/definitions/records/Record" }
        |    }
        |  },
        |  "definitions": {
        |    "records": {
        |      "Record": {
        |        "type": "object",
        |        "title": "Record",
        |        "properties": {
        |          "name":  { "type": "string" },
        |          "value": { "type": "number" }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "batch")
    val itemsType = serDe.schema.fields.find(_.name == "items").get.fieldType
    itemsType shouldBe a[ListType]
    val elemType = itemsType.asInstanceOf[ListType].elementType
    elemType shouldBe a[StructType]
    val elemStruct = elemType.asInstanceOf[StructType]
    elemStruct.fields.find(_.name == "name").get.fieldType shouldBe StringType
    elemStruct.fields.find(_.name == "value").get.fieldType shouldBe DoubleType
  }

  it should "fall back to StringType for an unresolvable $ref" in {
    val schema =
      """{
        |  "title": "broken",
        |  "type": "object",
        |  "properties": {
        |    "x": { "$ref": "#/definitions/missing/Type" }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "broken")
    serDe.schema.fields.find(_.name == "x").get.fieldType shouldBe StringType
  }

  it should "deserialize a nested struct accessed via $ref" in {
    val schema =
      """{
        |  "title": "event",
        |  "type": "object",
        |  "properties": {
        |    "id":   { "type": "string" },
        |    "meta": { "$ref": "#/definitions/common/Meta" }
        |  },
        |  "definitions": {
        |    "common": {
        |      "Meta": {
        |        "type": "object",
        |        "title": "Meta",
        |        "properties": {
        |          "region": { "type": "string" },
        |          "count":  { "type": "integer" }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val serDe = new JsonSchemaSerDe(schema, "event")
    val message = """{"id": "e1", "meta": {"region": "US", "count": 42}}"""
    val mutation = serDe.fromBytes(message.getBytes(StandardCharsets.UTF_8))

    val s = serDe.schema
    mutation.after(s.indexWhere(_.name == "id")) shouldBe "e1"

    val metaRow = mutation.after(s.indexWhere(_.name == "meta")).asInstanceOf[Array[Any]]
    val metaStruct = s.fields.find(_.name == "meta").get.fieldType.asInstanceOf[StructType]
    metaRow(metaStruct.indexWhere(_.name == "region")) shouldBe "US"
    metaRow(metaStruct.indexWhere(_.name == "count")) shouldBe 42L
  }
}
