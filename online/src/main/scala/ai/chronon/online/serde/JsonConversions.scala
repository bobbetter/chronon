package ai.chronon.online.serde

import ai.chronon.api._

import java.util
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Conversions between JSON Schema definitions / Jackson-parsed JSON objects and Chronon types.
  *
  * JSON Schema type mapping:
  *   integer -> LongType, number -> DoubleType, string -> StringType,
  *   boolean -> BooleanType, array -> ListType, object -> StructType or MapType
  *
  * Nullable types expressed as {"type": ["T", "null"]} are supported by extracting the non-null element.
  *
  * Format annotations:
  *   string + format:date             -> DateType
  *   string + format:date-time        -> TimestampType
  *   string + contentEncoding:base64  -> BinaryType
  *   number + format:decimal          -> DecimalType (precision/scale default to 38/18)
  *
  * Object types are resolved as:
  *   - StructType when "properties" is present
  *   - MapType when "additionalProperties" is present
  *   - MapType(StringType, StringType) as fallback
  */
object JsonConversions {

  /** Parses a JSON Schema object definition into a Chronon StructType.
    * The top-level schema map must have a "properties" key.
    * Falls back to `fallbackName` when the schema has no "title".
    */
  def toChrononSchema(schemaDef: util.Map[String, AnyRef], fallbackName: String): StructType = {
    val title = Option(schemaDef.get("title")).map(_.toString).getOrElse(fallbackName)
    // Root map is passed as-is so $ref resolution can reach the top-level "definitions" block
    parseObjectSchema(schemaDef, title, rootDefs = schemaDef)
  }

  /** Converts a parsed JSON object (as produced by Jackson) into a Chronon row array. */
  def toChrononRow(jsonMap: util.Map[String, AnyRef], structType: StructType): Array[Any] =
    structType.fields.map { field =>
      convertValue(jsonMap.get(field.name), field.fieldType)
    }

  private def parseObjectSchema(schema: util.Map[String, AnyRef],
                                 title: String,
                                 rootDefs: util.Map[String, AnyRef]): StructType = {
    val properties = Option(schema.get("properties"))
      .map(_.asInstanceOf[util.Map[String, AnyRef]])
      .getOrElse(util.Collections.emptyMap[String, AnyRef]())

    val fields = properties.asScala.map { case (fieldName, fieldDef) =>
      StructField(fieldName, jsonTypeToChronon(fieldDef.asInstanceOf[util.Map[String, AnyRef]], rootDefs))
    }.toArray

    StructType(title, fields)
  }

  // Visible for testing
  private[serde] def jsonTypeToChronon(fieldDef: util.Map[String, AnyRef]): DataType =
    jsonTypeToChronon(fieldDef, rootDefs = util.Collections.emptyMap())

  private def jsonTypeToChronon(fieldDef: util.Map[String, AnyRef],
                                 rootDefs: util.Map[String, AnyRef]): DataType = {
    // Resolve $ref before inspecting the type — format: "#/definitions/<group>/<TypeName>"
    if (fieldDef.containsKey("$ref")) {
      return resolveRef(fieldDef.get("$ref").toString, rootDefs) match {
        case Some(resolved) => jsonTypeToChronon(resolved, rootDefs)
        case None           => StringType
      }
    }

    // JSON Schema nullable fields use "type": ["string", "null"] — extract the non-null type
    val jsonType: String = fieldDef.get("type") match {
      case list: util.List[_] =>
        list.asScala.map(_.toString).find(_ != "null").getOrElse("string")
      case other =>
        Option(other).map(_.toString).getOrElse("string")
    }

    val format = Option(fieldDef.get("format")).map(_.toString)
    val contentEncoding = Option(fieldDef.get("contentEncoding")).map(_.toString)

    jsonType match {
      case "integer" => LongType
      case "number" =>
        format match {
          case Some("decimal") =>
            val precision = Option(fieldDef.get("precision")).map(_.toString.toInt).getOrElse(38)
            val scale = Option(fieldDef.get("scale")).map(_.toString.toInt).getOrElse(18)
            DecimalType(precision, scale)
          case _ => DoubleType
        }
      case "string" =>
        (format, contentEncoding) match {
          case (Some("date"), _)      => DateType
          case (Some("date-time"), _) => TimestampType
          case (_, Some("base64"))    => BinaryType
          case _                      => StringType
        }
      case "boolean" => BooleanType
      case "array" =>
        val items = Option(fieldDef.get("items"))
          .map(_.asInstanceOf[util.Map[String, AnyRef]])
          .getOrElse {
            val m = new util.LinkedHashMap[String, AnyRef]()
            m.put("type", "string")
            m
          }
        ListType(jsonTypeToChronon(items, rootDefs))
      case "object" =>
        if (fieldDef.containsKey("additionalProperties")) {
          val valueType = fieldDef.get("additionalProperties") match {
            case props: util.Map[_, _] =>
              jsonTypeToChronon(props.asInstanceOf[util.Map[String, AnyRef]], rootDefs)
            case _ => StringType
          }
          MapType(StringType, valueType)
        } else if (fieldDef.containsKey("properties")) {
          val nestedTitle = Option(fieldDef.get("title")).map(_.toString).getOrElse("nested")
          parseObjectSchema(fieldDef, nestedTitle, rootDefs)
        } else {
          MapType(StringType, StringType)
        }
      case _ => StringType
    }
  }

  /** Resolves a JSON Schema $ref of the form "#/definitions/<group>/<TypeName>" against the root schema map. */
  private def resolveRef(ref: String,
                          rootDefs: util.Map[String, AnyRef]): Option[util.Map[String, AnyRef]] = {
    // Only internal refs starting with "#/" are supported
    if (!ref.startsWith("#/")) return None
    val parts = ref.stripPrefix("#/").split("/")
    // Walk the root map one segment at a time
    var current: AnyRef = rootDefs
    for (part <- parts) {
      current = current match {
        case m: util.Map[_, _] => m.asInstanceOf[util.Map[String, AnyRef]].get(part)
        case _                 => null
      }
      if (current == null) return None
    }
    current match {
      case m: util.Map[_, _] => Some(m.asInstanceOf[util.Map[String, AnyRef]])
      case _                 => None
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
      case DateType =>
        value match {
          case s: String => java.sql.Date.valueOf(java.time.LocalDate.parse(s))
          case _         => null
        }
      case TimestampType =>
        value match {
          case s: String => java.sql.Timestamp.from(java.time.Instant.parse(s))
          case _         => null
        }
      case BinaryType =>
        value match {
          case s: String => util.Base64.getDecoder.decode(s)
          case _         => null
        }
      case DecimalType(_, _) =>
        value match {
          case s: String               => new java.math.BigDecimal(s)
          case d: java.lang.Double     => java.math.BigDecimal.valueOf(d)
          case i: java.lang.Integer    => new java.math.BigDecimal(i)
          case l: java.math.BigInteger => new java.math.BigDecimal(l)
          case l: java.lang.Long       => new java.math.BigDecimal(l)
          case _                       => null
        }
      case ListType(elemType) =>
        value match {
          case list: util.List[_] =>
            val result = new util.ArrayList[Any](list.size())
            list.forEach(elem => result.add(convertValue(elem, elemType)))
            result
          case _ => null
        }
      case MapType(keyType, valueType) =>
        value match {
          case map: util.Map[_, _] =>
            val result = new util.HashMap[Any, Any]()
            map.forEach((k, v) => result.put(convertValue(k, keyType), convertValue(v, valueType)))
            result
          case _ => null
        }
      case st: StructType =>
        value match {
          case map: util.Map[_, _] =>
            toChrononRow(map.asInstanceOf[util.Map[String, AnyRef]], st)
          case _ => null
        }
      case _ => value
    }
  }
}
