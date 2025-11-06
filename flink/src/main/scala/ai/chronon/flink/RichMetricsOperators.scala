package ai.chronon.flink

import ai.chronon.flink.deser.ProjectedEvent
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.util.Collector

/** Function to count late events.
  *
  * This function should consume the Side Output of the main tiling window.
  */
class LateEventCounter(featureGroupName: String) extends RichFlatMapFunction[ProjectedEvent, ProjectedEvent] {
  @transient private var lateEventCounter: Counter = _

  override def open(parameters: Configuration): Unit = {
    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", featureGroupName)
    lateEventCounter = metricsGroup.counter("tiling.late_events")
  }

  override def flatMap(in: ProjectedEvent, out: Collector[ProjectedEvent]): Unit = {
    lateEventCounter.inc()
    out.collect(in);
  }
}
