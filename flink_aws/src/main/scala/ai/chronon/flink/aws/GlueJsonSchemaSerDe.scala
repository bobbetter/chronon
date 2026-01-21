package ai.chronon.flink.aws

import ai.chronon.api.{
  BooleanType,
  DoubleType,
  FloatType,
  IntType,
  ListType,
  LongType,
  MapType,
  StringType,
  StructField,
  StructType
}
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{Mutation, SerDe}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.{GetSchemaVersionRequest, SchemaId}

import scala.util.Try

/**
 * SerDe implementation that fetches JSON Schema from AWS Glue Schema Registry
 * and deserializes JSON-encoded messages.
 *
 * This allows schema definitions to be managed centrally in Glue Schema Registry
 * rather than hardcoded in SerDe classes.
 *
 * Configure via topic string:
 *   kinesis://stream-name/serde=glue_json/glue_registry=chronon-registry/glue_schema=logins_event/region=us-east-2
 *
 * Parameters:
 *   - serde: Must be "glue_json" to use this SerDe
 *   - glue_registry: Name of the Glue Schema Registry
 *   - glue_schema: Name of the schema in the registry
 *   - region: AWS region (defaults to us-east-2)
 */
class GlueJsonSchemaSerDe(topicInfo: TopicInfo) extends SerDe {
  import GlueJsonSchemaSerDe._

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  private val regionName: String = topicInfo.params.getOrElse(RegionKey, "us-east-2")
  private val registryName: String =
    topicInfo.params.getOrElse(RegistryKey, throw new IllegalArgumentException(s"$RegistryKey not set in topic params"))
  private val schemaName: String =
    topicInfo.params.getOrElse(SchemaKey, throw new IllegalArgumentException(s"$SchemaKey not set in topic params"))

  // Lazily fetch and parse the schema from Glue
  lazy val (jsonSchemaDefinition, chrononSchema) = fetchAndParseSchema()

  private def fetchAndParseSchema(): (String, StructType) = {
    logger.info(s"Fetching schema from Glue: registry=$registryName, schema=$schemaName, region=$regionName")

    val glueClient = GlueClient.builder()
      .region(Region.of(regionName))
      .build()

    try {
      val schemaId = SchemaId.builder()
        .registryName(registryName)
        .schemaName(schemaName)
        .build()

      val request = GetSchemaVersionRequest.builder()
        .schemaId(schemaId)
        .schemaVersionNumber(software.amazon.awssdk.services.glue.model.SchemaVersionNumber.builder()
          .latestVersion(true)
          .build())
        .build()

      val response = glueClient.getSchemaVersion(request)
      val schemaDefinition = response.schemaDefinition()

      logger.info(s"Retrieved schema definition from Glue (version ${response.versionNumber()}): $schemaDefinition")

      val chrononSchema = parseJsonSchemaToChronon(schemaDefinition)
      (schemaDefinition, chrononSchema)
    } finally {
      glueClient.close()
    }
  }

  /**
   * Parse a JSON Schema definition into a Chronon StructType.
   * 
   * Supports JSON Schema draft-07 with basic types:
   *   - integer -> LongType (to handle large timestamps)
   *   - number -> DoubleType
   *   - string -> StringType
   *   - boolean -> BooleanType
   *   - array -> ListType
   *   - object -> StructType (nested) or MapType
   */
  private def parseJsonSchemaToChronon(schemaJson: String): StructType = {
    val parsed = parseJson(schemaJson)
    val title = parsed.getOrElse("title", schemaName).asInstanceOf[String]
    val properties = parsed.getOrElse("properties", Map.empty).asInstanceOf[Map[String, Any]]
    val required = parsed.getOrElse("required", List.empty).asInstanceOf[List[String]].toSet

    val fields = properties.map { case (fieldName, fieldDef) =>
      val fieldMap = fieldDef.asInstanceOf[Map[String, Any]]
      val dataType = jsonTypeToChronon(fieldMap)
      StructField(fieldName, dataType)
    }.toArray

    StructType(title, fields)
  }

  /**
   * Convert JSON Schema type to Chronon DataType.
   */
  private def jsonTypeToChronon(fieldDef: Map[String, Any]): ai.chronon.api.DataType = {
    val jsonType = fieldDef.getOrElse("type", "string").asInstanceOf[String]

    jsonType match {
      case "integer" => LongType // Use Long for integers to handle timestamps
      case "number"  => DoubleType
      case "string"  => StringType
      case "boolean" => BooleanType
      case "array" =>
        val items = fieldDef.getOrElse("items", Map("type" -> "string")).asInstanceOf[Map[String, Any]]
        ListType(jsonTypeToChronon(items))
      case "object" =>
        // Check if it's a map (additionalProperties) or a nested struct (properties)
        if (fieldDef.contains("additionalProperties")) {
          val valueType = fieldDef.get("additionalProperties") match {
            case Some(props: Map[_, _]) => jsonTypeToChronon(props.asInstanceOf[Map[String, Any]])
            case _ => StringType
          }
          MapType(StringType, valueType)
        } else if (fieldDef.contains("properties")) {
          parseJsonSchemaToChronon(fieldDef.toString) // Recursive for nested objects
        } else {
          MapType(StringType, StringType) // Default to string map
        }
      case _ => StringType // Default fallback
    }
  }

  override def schema: StructType = chrononSchema

  /**
   * Deserialize a JSON-encoded message using the schema fetched from Glue.
   */
  override def fromBytes(bytes: Array[Byte]): Mutation = {
    val json = new String(bytes, java.nio.charset.StandardCharsets.UTF_8).trim
    val parsed = parseJson(json)

    // Build the row array in schema field order
    val row: Array[Any] = chrononSchema.fields.map { field =>
      val rawValue = parsed.get(field.name).orNull
      convertValue(rawValue, field.fieldType)
    }

    Mutation(schema, null, row)
  }

  /**
   * Convert a parsed JSON value to the expected Chronon type.
   */
  private def convertValue(value: Any, targetType: ai.chronon.api.DataType): Any = {
    if (value == null) return null

    targetType match {
      case LongType =>
        value match {
          case l: java.lang.Long    => l
          case i: java.lang.Integer => i.toLong
          case d: java.lang.Double  => d.toLong
          case s: String            => Try(s.toLong).getOrElse(null)
          case _                    => null
        }
      case IntType =>
        value match {
          case i: java.lang.Integer => i
          case l: java.lang.Long    => l.toInt
          case d: java.lang.Double  => d.toInt
          case s: String            => Try(s.toInt).getOrElse(null)
          case _                    => null
        }
      case DoubleType =>
        value match {
          case d: java.lang.Double  => d
          case f: java.lang.Float   => f.toDouble
          case l: java.lang.Long    => l.toDouble
          case i: java.lang.Integer => i.toDouble
          case s: String            => Try(s.toDouble).getOrElse(null)
          case _                    => null
        }
      case FloatType =>
        value match {
          case f: java.lang.Float   => f
          case d: java.lang.Double  => d.toFloat
          case l: java.lang.Long    => l.toFloat
          case i: java.lang.Integer => i.toFloat
          case s: String            => Try(s.toFloat).getOrElse(null)
          case _                    => null
        }
      case StringType =>
        value match {
          case s: String            => s
          case n: java.lang.Number  => String.valueOf(n)
          case other                => String.valueOf(other)
        }
      case BooleanType =>
        value match {
          case b: java.lang.Boolean => b
          case s: String            => java.lang.Boolean.valueOf(s)
          case _                    => null
        }
      case ListType(elemType) =>
        value match {
          case list: java.util.List[_] =>
            val result = new java.util.ArrayList[Any]()
            list.forEach(elem => result.add(convertValue(elem, elemType)))
            result
          case list: List[_] =>
            val javaList = new java.util.ArrayList[Any]()
            list.foreach(elem => javaList.add(convertValue(elem, elemType)))
            javaList
          case _ => null
        }
      case MapType(keyType, valueType) =>
        value match {
          case map: java.util.Map[_, _] =>
            val result = new java.util.HashMap[Any, Any]()
            map.forEach((k, v) => result.put(convertValue(k, keyType), convertValue(v, valueType)))
            result
          case map: Map[_, _] =>
            val javaMap = new java.util.HashMap[Any, Any]()
            map.foreach { case (k, v) => javaMap.put(convertValue(k, keyType), convertValue(v, valueType)) }
            javaMap
          case _ => null
        }
      case _ => value
    }
  }

  // ============================================
  // Simple JSON Parser (no external dependencies)
  // ============================================

  /**
   * Minimal JSON parser for flat and nested objects.
   * Handles: strings, numbers, booleans, null, arrays, nested objects.
   */
  private def parseJson(input: String): Map[String, Any] = {
    val trimmed = input.trim
    if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
      return Map.empty
    }
    parseObject(trimmed)._1
  }

  private def parseObject(s: String): (Map[String, Any], String) = {
    var remaining = s.trim.stripPrefix("{").trim
    val result = scala.collection.mutable.LinkedHashMap[String, Any]()

    while (remaining.nonEmpty && !remaining.startsWith("}")) {
      // Skip comma
      if (remaining.startsWith(",")) {
        remaining = remaining.drop(1).trim
      }

      // Parse key
      val (key, afterKey) = parseString(remaining)
      remaining = afterKey.trim

      // Skip colon
      if (remaining.startsWith(":")) {
        remaining = remaining.drop(1).trim
      }

      // Parse value
      val (value, afterValue) = parseValue(remaining)
      result(key) = value
      remaining = afterValue.trim
    }

    (result.toMap, remaining.stripPrefix("}").trim)
  }

  private def parseArray(s: String): (List[Any], String) = {
    var remaining = s.trim.stripPrefix("[").trim
    val result = scala.collection.mutable.ListBuffer[Any]()

    while (remaining.nonEmpty && !remaining.startsWith("]")) {
      // Skip comma
      if (remaining.startsWith(",")) {
        remaining = remaining.drop(1).trim
      }

      val (value, afterValue) = parseValue(remaining)
      result += value
      remaining = afterValue.trim
    }

    (result.toList, remaining.stripPrefix("]").trim)
  }

  private def parseString(s: String): (String, String) = {
    val trimmed = s.trim
    if (!trimmed.startsWith("\"")) {
      throw new IllegalArgumentException(s"Expected string starting with quote, got: ${trimmed.take(20)}")
    }

    var i = 1
    var escaped = false
    val sb = new StringBuilder

    while (i < trimmed.length) {
      val c = trimmed.charAt(i)
      if (escaped) {
        c match {
          case 'n'  => sb.append('\n')
          case 'r'  => sb.append('\r')
          case 't'  => sb.append('\t')
          case '\\' => sb.append('\\')
          case '"'  => sb.append('"')
          case '/'  => sb.append('/')
          case _    => sb.append(c)
        }
        escaped = false
      } else if (c == '\\') {
        escaped = true
      } else if (c == '"') {
        return (sb.toString(), trimmed.drop(i + 1))
      } else {
        sb.append(c)
      }
      i += 1
    }

    throw new IllegalArgumentException("Unterminated string")
  }

  private def parseValue(s: String): (Any, String) = {
    val trimmed = s.trim

    if (trimmed.startsWith("\"")) {
      parseString(trimmed)
    } else if (trimmed.startsWith("{")) {
      parseObject(trimmed)
    } else if (trimmed.startsWith("[")) {
      parseArray(trimmed)
    } else if (trimmed.startsWith("null")) {
      (null, trimmed.drop(4))
    } else if (trimmed.startsWith("true")) {
      (java.lang.Boolean.TRUE, trimmed.drop(4))
    } else if (trimmed.startsWith("false")) {
      (java.lang.Boolean.FALSE, trimmed.drop(5))
    } else {
      // Parse number
      val numEnd = trimmed.indexWhere(c => !c.isDigit && c != '.' && c != '-' && c != '+' && c != 'e' && c != 'E')
      val numStr = if (numEnd == -1) trimmed else trimmed.take(numEnd)
      val remaining = if (numEnd == -1) "" else trimmed.drop(numEnd)

      val number: Any = if (numStr.contains('.') || numStr.toLowerCase.contains('e')) {
        java.lang.Double.valueOf(numStr)
      } else {
        Try(java.lang.Long.valueOf(numStr)).getOrElse(java.lang.Double.valueOf(numStr))
      }

      (number, remaining)
    }
  }
}

object GlueJsonSchemaSerDe {
  val RegistryKey = "glue_registry"
  val SchemaKey = "glue_schema"
  val RegionKey = "region"
}
