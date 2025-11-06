package ai.chronon.spark.udafs

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

// we assume that inputs are cast to strings prior to passing into the aggregator
trait BaseHistogramAggregator[IN] extends Aggregator[IN, Map[String, Long], Map[String, Long]] {
  def zero: Map[String, Long] = Map.empty[String, Long]

  def merge(acc1: Map[String, Long], acc2: Map[String, Long]): Map[String, Long] = {
    if (acc1 == null || acc1.isEmpty) acc2
    else if (acc2 == null || acc2.isEmpty) acc1
    else acc1 ++ acc2.map { case (k, v) => k -> (acc1.getOrElse(k, 0L) + v) }
  }

  def finish(acc: Map[String, Long]): Map[String, Long] = acc

  def bufferEncoder: Encoder[Map[String, Long]] = ExpressionEncoder()
  def outputEncoder: Encoder[Map[String, Long]] = ExpressionEncoder()

  protected def updateHistogram(acc: Map[String, Long], key: String, value: Long = 1L): Map[String, Long] = {
    val effectiveKey = if (key == null) "NULL" else key
    acc + (effectiveKey -> (acc.getOrElse(effectiveKey, 0L) + value))
  }
}

object MapHistogramAggregator extends BaseHistogramAggregator[Map[String, Long]] {
  def reduce(acc: Map[String, Long], input: Map[String, Long]): Map[String, Long] = {
    merge(acc, input)
  }
}

object HistogramAggregator extends BaseHistogramAggregator[String] {
  def reduce(acc: Map[String, Long], input: String): Map[String, Long] = {
    updateHistogram(acc, input)
  }
}

object ArrayStringHistogramAggregator extends BaseHistogramAggregator[Seq[String]] {
  def reduce(acc: Map[String, Long], input: Seq[String]): Map[String, Long] = {
    if (input == null || input.isEmpty) acc
    else input.foldLeft(acc)((acc, str) => updateHistogram(acc, str))
  }
}
