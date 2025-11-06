package ai.chronon.online.stats

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.Window
import ai.chronon.observability.DriftMetric
import ai.chronon.observability.TileDrift
import ai.chronon.observability.TileSummary
import ai.chronon.online.stats.DriftMetrics.histogramDistance
import ai.chronon.online.stats.DriftMetrics.percentileDistance

object TileDriftCalculator {

  @inline
  private def toArray(l: java.util.List[java.lang.Double]): Array[Double] = {
    l.iterator().toScala.map(_.toDouble).toArray
  }

  @inline
  private def toArray(l: java.util.List[java.lang.Integer]): Array[Int] = {
    l.iterator().toScala.map(_.toInt).toArray
  }

  // a is after, b is before
  def between(a: TileSummary, b: TileSummary, metric: DriftMetric = DriftMetric.JENSEN_SHANNON): TileDrift = {
    val result = new TileDrift()

    if (a.isSetPercentiles && b.isSetPercentiles) {
      val drift = percentileDistance(toArray(a.getPercentiles), toArray(b.getPercentiles), metric)
      result.setPercentileDrift(drift)
    }

    if (a.isSetHistogram && b.isSetHistogram) {
      val drift = histogramDistance(a.getHistogram, b.getHistogram, metric)
      result.setHistogramDrift(drift)
    }

    if (a.isSetCount && b.isSetCount && b.getCount != 0.0) {
      // preserving sign, since direction of change is usually relevant
      val change = (a.getCount - b.getCount).toDouble / b.getCount
      result.setCountChangePercent(change * 100)
    }

    if (a.isSetNullCount && b.isSetNullCount && b.getNullCount != 0) {
      // preserving sign, since direction of change is usually relevant
      val aNullRatio = a.getNullCount.toDouble / a.getCount.toDouble
      val bNullRatio = b.getNullCount.toDouble / b.getCount.toDouble
      val change = (aNullRatio - bNullRatio) / bNullRatio
      result.setNullRatioChangePercent(change * 100)
    }

    if (a.isSetInnerCount && b.isSetInnerCount && b.getInnerCount != 0.0) {
      // preserving sign, since direction of change is usually relevant
      val change = (a.getInnerCount - b.getInnerCount).toDouble / b.getInnerCount
      result.setInnerCountChangePercent(change * 100)
    }

    if (a.isSetInnerNullCount && b.isSetInnerNullCount && b.getInnerNullCount != 0.0) {
      // preserving sign, since direction of change is usually relevant
      val aNullRatio = a.getInnerNullCount.toDouble / a.getInnerCount.toDouble
      val bNullRatio = b.getInnerNullCount.toDouble / b.getInnerCount.toDouble
      val change = (aNullRatio - bNullRatio) / bNullRatio
      result.setInnerNullCountChangePercent(change * 100)
    }

    if (a.isSetLengthPercentiles && b.isSetLengthPercentiles) {
      val drift = percentileDistance(toArray(a.getLengthPercentiles).map(_.toDouble),
                                     toArray(b.getLengthPercentiles).map(_.toDouble),
                                     metric)
      result.setLengthPercentilesDrift(drift)
    }

    if (a.isSetStringLengthPercentiles && b.isSetStringLengthPercentiles) {
      val drift = percentileDistance(toArray(a.getStringLengthPercentiles).map(_.toDouble),
                                     toArray(b.getStringLengthPercentiles).map(_.toDouble),
                                     metric)
      result.setStringLengthPercentilesDrift(drift)
    }

    result
  }

  // for each summary with ts >= startMs, use spec.lookBack to find the previous summary and calculate drift
  // we do this by first creating a map of summaries by timestamp
  def toTileDrifts(summariesWithTimestamps: Array[(TileSummary, Long)],
                   metric: DriftMetric,
                   startMs: Long,
                   lookBackWindow: Window): Array[(TileDrift, Long)] = {
    // TODO-optimization - this can be done in one pass instead of two, optimize if this is a bottleneck
    val summariesByTimestamp = summariesWithTimestamps.iterator.map { case (summary, ts) => ts -> summary }.toMap
    val lookBackMs = lookBackWindow.millis

    summariesWithTimestamps.iterator
      .filter { case (_, ts) => ts >= startMs }
      .map { case (summary, ts) =>
        val previousTs = startMs + (2 * WindowUtils.Day.millis)
        val previousSummary = summariesByTimestamp.get(previousTs)
        val drift = previousSummary.map(between(summary, _, metric)).getOrElse(new TileDrift())
        drift -> ts
      }
      .toArray
  }
}
