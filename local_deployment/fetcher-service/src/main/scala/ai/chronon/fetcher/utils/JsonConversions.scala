package ai.chronon.fetcher.utils

import io.circe.Json
import scala.jdk.CollectionConverters._

object JsonConversions {

  def jsonToAnyRef(json: Json): AnyRef =
    json.fold[AnyRef](
      null,
      bool => Boolean.box(bool),
      num =>
        num.toBigDecimal
          .map { bd =>
            bd.toBigIntExact match {
              case Some(bigInt) if bigInt.isValidInt  => Int.box(bigInt.intValue())
              case Some(bigInt) if bigInt.isValidLong => Long.box(bigInt.longValue())
              case Some(bigInt)                        => bigInt.bigInteger
              case None                                => bd.bigDecimal
            }
          }
          .getOrElse(Double.box(num.toDouble)),
      str => str,
      arr => arr.map(jsonToAnyRef).toVector,
      obj => obj.toMap.map { case (k, v) => k -> jsonToAnyRef(v) }
    )

  def anyToJson(value: Any): Json = value match {
    case null                     => Json.Null
    case json: Json               => json
    case s: String                => Json.fromString(s)
    case b: java.lang.Boolean     => Json.fromBoolean(b)
    case i: java.lang.Integer     => Json.fromInt(i)
    case l: java.lang.Long        => Json.fromLong(l)
    case s: java.lang.Short       => Json.fromInt(s.toInt)
    case d: java.lang.Double      => Json.fromDoubleOrNull(d)
    case f: java.lang.Float       => Json.fromFloatOrNull(f)
    case bd: java.math.BigDecimal => Json.fromBigDecimal(bd)
    case bd: scala.math.BigDecimal => Json.fromBigDecimal(bd.bigDecimal)
    case bi: java.math.BigInteger  => Json.fromBigInt(BigInt(bi))
    case n: java.lang.Number       => Json.fromDoubleOrNull(n.doubleValue())
    case map: Map[_, _] =>
      Json.obj(map.collect { case (k: String, v) => k -> anyToJson(v) }.toSeq: _*)
    case map: java.util.Map[_, _] =>
      Json.obj(map.asScala.collect { case (k, v) => k.toString -> anyToJson(v) }.toSeq: _*)
    case iterable: java.lang.Iterable[_] =>
      Json.arr(iterable.asScala.map(anyToJson).toSeq: _*)
    case iterable: Iterable[_] =>
      Json.arr(iterable.map(anyToJson).toSeq: _*)
    case array: Array[_] =>
      Json.arr(array.map(anyToJson): _*)
    case other => Json.fromString(other.toString)
  }
}
