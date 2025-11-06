package ai.chronon.spark.udafs

import ai.chronon.aggregator.base.CpcFriendly
import ai.chronon.api.ColorPrinter.ColorString
import org.apache.datasketches.cpc.CpcSketch
import org.apache.datasketches.cpc.CpcUnion
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.map_values
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.types._

import scala.util.Try

abstract class ApproxDistinct[IN](lgK: Int = 8) extends Aggregator[IN, CpcSketch, Long] {

  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  override def zero: CpcSketch = new CpcSketch(lgK)

  // Merge two intermediate values
  override def merge(b1: CpcSketch, b2: CpcSketch): CpcSketch = {
    val union = new CpcUnion(lgK)
    union.update(b1)
    union.update(b2)
    union.getResult
  }

  // Transform the output of the reduction
  override def finish(reduction: CpcSketch): Long = reduction.getEstimate.toLong

  // this should pick up cpc sketch serialization logic from ChrononKryoRegistrator class when encoders are specified
  override def bufferEncoder: Encoder[CpcSketch] = Encoders.kryo(classOf[CpcSketch])

  // Specifies the Encoder for the final output value type
  override def outputEncoder: Encoder[Long] = ExpressionEncoder()
}

class ScalarApproxDistinct[IN: CpcFriendly](lgK: Int = 8) extends ApproxDistinct[IN](lgK) {
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: CpcSketch, input: IN): CpcSketch = {
    if (input != null)
      implicitly[CpcFriendly[IN]].update(buffer, input)
    buffer
  }
}

class ArrayApproxDistinct[IN: CpcFriendly](lgK: Int = 8) extends ApproxDistinct[Seq[IN]](lgK) {
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: CpcSketch, input: Seq[IN]): CpcSketch = {
    if (input == null) return buffer
    input.iterator.filter(_ != null).foreach(implicitly[CpcFriendly[IN]].update(buffer, _))
    buffer
  }
}

object ApproxDistinct {
  def udafOf(dataType: DataType): UserDefinedFunction = {
    dataType match {
      case IntegerType => udaf(new ScalarApproxDistinct[Int]())
      case FloatType   => udaf(new ScalarApproxDistinct[Float]())
      case LongType    => udaf(new ScalarApproxDistinct[Long]())
      case DoubleType  => udaf(new ScalarApproxDistinct[Double]())
      case StringType  => udaf(new ScalarApproxDistinct[String]())
      case ArrayType(valueType, _) =>
        valueType match {
          case IntegerType => udaf(new ArrayApproxDistinct[Int]())
          case FloatType   => udaf(new ArrayApproxDistinct[Float]())
          case LongType    => udaf(new ArrayApproxDistinct[Long]())
          case DoubleType  => udaf(new ArrayApproxDistinct[Double]())
          case StringType  => udaf(new ArrayApproxDistinct[String]())
          case ByteType    => udaf(new ScalarApproxDistinct[Array[Byte]]())
          case _           => throw new UnsupportedOperationException(s"Unsupported array type $valueType")
        }
      case _ => throw new UnsupportedOperationException(s"Unsupported type $dataType")
    }
  }

  def columnCardinality(df: DataFrame): Map[String, Long] = {
    val aggs: Array[Column] = df.schema.fields.flatMap { field =>
      val expr = Try {
        // we count uniques of map's value types only, keys are assumed to be categorical
        val (inner, effectiveType) = field.dataType match {
          case MapType(_, v, _) => map_values(df(field.name)) -> ArrayType(v)
          case t                => df(field.name) -> t
        }
        val aggFunc = ApproxDistinct.udafOf(effectiveType)
        aggFunc(inner).as(s"${field.name}_count")
      }

      // struct types, among others, are not yet supported
      if (expr.isFailure) {
        println(s"Failed to aggregate column ${field.name} of type ${field.dataType}".red)
        println(expr.failed.get.getMessage.red)
        None
      } else {
        Some(expr.get)
      }
    }

    val aggDf = df.agg(aggs.head, aggs.tail: _*)
    val row = aggDf.collect().head
    df.schema.fields.map { field =>
      field.name -> row.getAs[Long](s"${field.name}_count")
    }.toMap
  }
}
