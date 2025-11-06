package ai.chronon.online.stats
import ai.chronon.observability.DriftMetric

import scala.math._

object DriftMetrics {
  def percentileDistance(a: Array[Double], b: Array[Double], metric: DriftMetric, debug: Boolean = false): Double = {
    val breaks = (a ++ b).sorted.distinct
    val aProjected = AssignIntervals.on(a, breaks)
    val bProjected = AssignIntervals.on(b, breaks)

    val aNormalized = normalize(aProjected)
    val bNormalized = normalize(bProjected)

    val func = termFunc(metric)

    var i = 0
    var result = 0.0

    while (i < aNormalized.length) {
      val ai = aNormalized(i)
      val bi = bNormalized(i)
      val delta = func(ai, bi)

      result += delta
      i += 1
    }

    if (debug) {
      def printArr(arr: Array[Double]): String =
        arr.map(v => f"$v%.3f").mkString(", ")
      println(f"""
           |aProjected : ${printArr(aProjected)}
           |bProjected : ${printArr(bProjected)}
           |aNormalized: ${printArr(aNormalized)}
           |bNormalized: ${printArr(bNormalized)}
           |result     : $result%.4f
           |""".stripMargin)
    }
    result
  }

  // java map is what thrift produces upon deserialization
  type Histogram = java.util.Map[String, java.lang.Long]
  def histogramDistance(a: Histogram, b: Histogram, metric: DriftMetric): Double = {

    @inline def sumValues(h: Histogram): Double = {
      var result = 0.0
      val it = h.entrySet().iterator()
      while (it.hasNext) {
        result += it.next().getValue
      }
      result
    }
    val aSum = sumValues(a)
    val bSum = sumValues(b)

    val aIt = a.entrySet().iterator()
    var result = 0.0
    val func = termFunc(metric)
    while (aIt.hasNext) {
      val entry = aIt.next()
      val key = entry.getKey
      val aVal = entry.getValue.toDouble
      val bValOpt: java.lang.Long = b.get(key)
      val bVal: Double = if (bValOpt == null) 0.0 else bValOpt.toDouble
      val term = func(aVal / aSum, bVal / bSum)
      result += term
    }

    val bIt = b.entrySet().iterator()
    while (bIt.hasNext) {
      val entry = bIt.next()
      val key = entry.getKey
      val bVal = entry.getValue.toDouble
      val aValOpt = a.get(key)
      if (aValOpt == null) {
        val term = func(0.0, bVal / bSum)
        result += term
      }
    }

    result
  }

  @inline
  private def normalize(arr: Array[Double]): Array[Double] = {
    // TODO-OPTIMIZATION: normalize in place instead if this is a hotspot
    val result = Array.ofDim[Double](arr.length)
    val sum = arr.sum
    var i = 0
    while (i < arr.length) {
      result.update(i, arr(i) / sum)
      i += 1
    }
    result
  }

  @inline
  private def klDivergenceTerm(a: Double, b: Double): Double = {
    if (a > 0 && b > 0) a * math.log(a / b) else 0
  }

  @inline
  private def jsdTerm(a: Double, b: Double): Double = {
    val m = (a + b) * 0.5
    (klDivergenceTerm(a, m) + klDivergenceTerm(b, m)) * 0.5
  }

  @inline
  private def hellingerTerm(a: Double, b: Double): Double = {
    pow(sqrt(a) - sqrt(b), 2) * 0.5
  }

  @inline
  private def psiTerm(a: Double, b: Double): Double = {
    val aFixed = if (a == 0.0) 1e-5 else a
    val bFixed = if (b == 0.0) 1e-5 else b
    (bFixed - aFixed) * log(bFixed / aFixed)
  }

  @inline
  private def termFunc(d: DriftMetric): (Double, Double) => Double =
    d match {
      case DriftMetric.PSI            => psiTerm
      case DriftMetric.HELLINGER      => hellingerTerm
      case DriftMetric.JENSEN_SHANNON => jsdTerm
    }

  case class Thresholds(moderate: Double, severe: Double) {
    def str(driftScore: Double): String = {
      if (driftScore < moderate) "LOW"
      else if (driftScore < severe) "MODERATE"
      else "SEVERE"
    }
  }

  @inline
  def thresholds(d: DriftMetric): Thresholds =
    d match {
      case DriftMetric.JENSEN_SHANNON => Thresholds(0.05, 0.15)
      case DriftMetric.HELLINGER      => Thresholds(0.05, 0.15)
      case DriftMetric.PSI            => Thresholds(0.1, 0.2)
    }
}
