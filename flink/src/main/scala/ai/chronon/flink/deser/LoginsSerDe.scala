package ai.chronon.flink.deser

import ai.chronon.api.{StructField => ZStructField, StructType => ZStructType, LongType => ZLongType, DoubleType => ZDoubleType, StringType => ZStringType}
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{Mutation, SerDe}

import scala.util.Try

/** SerDe for the logins events when messages are simple JSON objects with fields:
  *   - ts: Long (event time in millis)
  *   - event_id: String
  *   - user_id: String (or numeric convertible to String)
  *   - login_method: String
  *   - device_type: String
  *   - ip_address: String
  *
  * If you use Avro on the wire, consider wrapping an AvroSerDe with your Avro schema instead.
  */
class LoginsSerDe(topicInfo: TopicInfo) extends SerDe {

  private val zSchema: ZStructType = ZStructType(
    "logins_event",
    Array(
      ZStructField("ts", ZLongType),
      ZStructField("event_id", ZStringType),
      ZStructField("user_id", ZStringType),
      ZStructField("login_method", ZStringType),
      ZStructField("device_type", ZStringType),
      ZStructField("ip_address", ZStringType)
    )
  )

  override def schema: ZStructType = zSchema

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    val json = new String(bytes, java.nio.charset.StandardCharsets.UTF_8).trim
    val parsed = parseFlatJson(json)
    val row: Array[Any] = Array[Any](
      parsed.getOrElse("ts", null).asInstanceOf[java.lang.Long],
      toStringOrNull(parsed.get("event_id")),
      toStringOrNull(parsed.get("user_id")),
      toStringOrNull(parsed.get("login_method")),
      toStringOrNull(parsed.get("device_type")),
      toStringOrNull(parsed.get("ip_address"))
    )
    Mutation(schema, null, row)
  }

  private def toStringOrNull(v: Option[Any]): String = v match {
    case Some(s: String) => s
    case Some(n: java.lang.Number) => String.valueOf(n)
    case Some(other) => String.valueOf(other)
    case None => null
  }

  /** Minimal, dependency-free flat JSON parser for simple key-value objects.
    * Accepts numbers, strings and booleans; strings must be quoted.
    * Not suitable for nested objects or arrays.
    */
  private def parseFlatJson(input: String): Map[String, Any] = {
    // strip outer braces
    val s = input.dropWhile(_ != '{').drop(1).reverse.dropWhile(_ != '}').drop(1).reverse
    if (s.trim.isEmpty) return Map.empty
    val parts = splitTopLevel(s)
    parts.flatMap { kv =>
      val idx = kv.indexOf(":")
      if (idx <= 0) None
      else {
        val key = unquote(kv.substring(0, idx).trim)
        val raw = kv.substring(idx + 1).trim
        Some(key -> parseValue(raw))
      }
    }.toMap
  }

  private def splitTopLevel(s: String): Seq[String] = {
    val buf = new StringBuilder
    var inString = false
    var esc = false
    val out = scala.collection.mutable.ArrayBuffer.empty[String]
    s.foreach { ch =>
      if (esc) { buf.append(ch); esc = false }
      else ch match {
        case '\\' if inString => buf.append(ch); esc = true
        case '"' => inString = !inString; buf.append(ch)
        case ',' if !inString => out += buf.result(); buf.clear()
        case c => buf.append(c)
      }
    }
    val last = buf.result().trim
    if (last.nonEmpty) out += last
    out.toSeq
  }

  private def unquote(s: String): String = {
    val t = s.trim
    if (t.startsWith("\"") && t.endsWith("\"")) t.substring(1, t.length - 1) else t
  }

  private def parseValue(raw: String): Any = {
    if (raw == "null") null
    else if (raw == "true" || raw == "false") java.lang.Boolean.valueOf(raw)
    else if (raw.startsWith("\"") && raw.endsWith("\"")) unquote(raw)
    else Try(java.lang.Long.valueOf(raw)).orElse(Try(java.lang.Double.valueOf(raw))).getOrElse(raw)
  }
}