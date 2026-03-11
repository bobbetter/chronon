package ai.chronon.flink_connectors.kinesis

import ai.chronon.api._
import ai.chronon.online.serde.{Mutation, SerDe}
import com.fasterxml.jackson.databind.ObjectMapper

import scala.jdk.CollectionConverters._
import scala.util.Try

/** SerDe for JSON-encoded messages with schemas defined in JSON Schema format.
  * Used by [[GlueSchemaSerDe]] when the Glue registry schema format is JSON (not Avro).
  *
  * Supports JSON Schema types:
  *   integer -> LongType, number -> DoubleType, string -> StringType,
  *   boolean -> BooleanType, array -> ListType, object -> StructType or MapType
  *
  * Object types are resolved as:
  *   - StructType when "properties" is present
  *   - MapType when "additionalProperties" is present
  *   - MapType(StringType, StringType) as fallback
  */
class JsonSchemaSerDe(jsonSchemaDefinition: String, schemaName: String) extends SerDe {

  @transient private lazy val objectMapper = new ObjectMapper()

  private lazy val chrononSchema: StructType = {
    val schemaDef = objectMapper.readValue(jsonSchemaDefinition, classOf[java.util.LinkedHashMap[String, AnyRef]])
    val title = Option(schemaDef.get("title")).map(_.toString).getOrElse(schemaName)
    parseObjectSchema(schemaDef, title)
  }

  override def schema: StructType = chrononSchema

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    val parsed = objectMapper.readValue(bytes, classOf[java.util.LinkedHashMap[String, AnyRef]])
    val row = buildRow(parsed, chrononSchema)
    Mutation(schema, null, row)
  }

  private def buildRow(jsonMap: java.util.Map[String, AnyRef], structType: StructType): Array[Any] =
    structType.fields.map { field =>
      convertValue(jsonMap.get(field.name), field.fieldType)
    }

  private def parseObjectSchema(schema: java.util.Map[String, AnyRef], title: String): StructType = {
    val properties = Option(schema.get("properties"))
      .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
      .getOrElse(java.util.Collections.emptyMap[String, AnyRef]())

    val fields = properties.asScala.map { case (fieldName, fieldDef) =>
      val fieldMap = fieldDef.asInstanceOf[java.util.Map[String, AnyRef]]
      StructField(fieldName, jsonTypeToChronon(fieldMap))
    }.toArray

    StructType(title, fields)
  }

  private def jsonTypeToChronon(fieldDef: java.util.Map[String, AnyRef]): DataType = {
    val jsonType = Option(fieldDef.get("type")).map(_.toString).getOrElse("string")

    jsonType match {
      case "integer" => LongType
      case "number"  => DoubleType
      case "string"  => StringType
      case "boolean" => BooleanType
      case "array" =>
        val items = Option(fieldDef.get("items"))
          .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
          .getOrElse {
            val m = new java.util.LinkedHashMap[String, AnyRef]()
            m.put("type", "string")
            m
          }
        ListType(jsonTypeToChronon(items))
      case "object" =>
        if (fieldDef.containsKey("additionalProperties")) {
          val valueType = fieldDef.get("additionalProperties") match {
            case props: java.util.Map[_, _] =>
              jsonTypeToChronon(props.asInstanceOf[java.util.Map[String, AnyRef]])
            case _ => StringType
          }
          MapType(StringType, valueType)
        } else if (fieldDef.containsKey("properties")) {
          val nestedTitle = Option(fieldDef.get("title")).map(_.toString).getOrElse("nested")
          parseObjectSchema(fieldDef.asInstanceOf[java.util.Map[String, AnyRef]], nestedTitle)
        } else {
          MapType(StringType, StringType)
        }
      case _ => StringType
    }
  }

  private def convertValue(value: Any, targetType: DataType): Any = {
    if (value == null) return null

    targetType match {
      case LongType =>
        value match {
          case l: java.lang.Long    => l
          case i: java.lang.Integer => i.toLong: java.lang.Long
          case d: java.lang.Double  => d.toLong: java.lang.Long
          case s: String            => Try(java.lang.Long.valueOf(s)).getOrElse(null)
          case _                    => null
        }
      case IntType =>
        value match {
          case i: java.lang.Integer => i
          case l: java.lang.Long    => l.toInt: java.lang.Integer
          case d: java.lang.Double  => d.toInt: java.lang.Integer
          case s: String            => Try(java.lang.Integer.valueOf(s)).getOrElse(null)
          case _                    => null
        }
      case DoubleType =>
        value match {
          case d: java.lang.Double  => d
          case f: java.lang.Float   => f.toDouble: java.lang.Double
          case l: java.lang.Long    => l.toDouble: java.lang.Double
          case i: java.lang.Integer => i.toDouble: java.lang.Double
          case s: String            => Try(java.lang.Double.valueOf(s)).getOrElse(null)
          case _                    => null
        }
      case FloatType =>
        value match {
          case f: java.lang.Float   => f
          case d: java.lang.Double  => d.toFloat: java.lang.Float
          case l: java.lang.Long    => l.toFloat: java.lang.Float
          case i: java.lang.Integer => i.toFloat: java.lang.Float
          case s: String            => Try(java.lang.Float.valueOf(s)).getOrElse(null)
          case _                    => null
        }
      case StringType =>
        String.valueOf(value)
      case BooleanType =>
        value match {
          case b: java.lang.Boolean => b
          case s: String            => java.lang.Boolean.valueOf(s)
          case _                    => null
        }
      case ListType(elemType) =>
        value match {
          case list: java.util.List[_] =>
            val result = new java.util.ArrayList[Any](list.size())
            list.forEach(elem => result.add(convertValue(elem, elemType)))
            result
          case _ => null
        }
      case MapType(keyType, valueType) =>
        value match {
          case map: java.util.Map[_, _] =>
            val result = new java.util.HashMap[Any, Any]()
            map.forEach((k, v) => result.put(convertValue(k, keyType), convertValue(v, valueType)))
            result
          case _ => null
        }
      case st: StructType =>
        value match {
          case map: java.util.Map[_, _] =>
            buildRow(map.asInstanceOf[java.util.Map[String, AnyRef]], st)
          case _ => null
        }
      case _ => value
    }
  }
}
