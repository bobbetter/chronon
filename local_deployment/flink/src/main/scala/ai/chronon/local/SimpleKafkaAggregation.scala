package ai.chronon.local

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.time.Deadline
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.util.Collector

/**
  * Reads JSON lines like {"user":"u1", "val": 3} from a Kafka source topic,
  * sums values per user in a tumbling processing-time window, and writes "user,sum" lines to a sink topic.
  */
object SimpleKafkaAggregation {

  final case class Event(user: String, value: Long)
  final case class Conf(
      bootstrap: String = "kafka:9092",
      sourceTopic: String = "source-events",
      sinkTopic: String = "sink-aggregates",
      windowSec: Int = 100
  )

  private[local] def parseJsonLine(line: String): Option[Event] = {
    // very small hand-rolled parser to avoid json deps
    // expects keys "user" and optional "val"/"value"
    val trimmed = line.trim
    if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) return None
    val body = trimmed.substring(1, trimmed.length - 1)
    val parts = body.split(",").map(_.trim)
    var user: Option[String] = None
    var value: Long = 1L
    parts.foreach { p =>
      val kv = p.split(":", 2).map(_.trim)
      if (kv.length == 2) {
        val key = kv(0).stripPrefix("\"").stripSuffix("\"")
        val raw = kv(1)
        key match {
          case "user" =>
            user = Some(raw.stripPrefix("\"").stripSuffix("\""))
          case "val" | "value" =>
            val digits = raw.takeWhile(c => c.isDigit || c == '-' )
            if (digits.nonEmpty) value = digits.toLong
          case _ =>
        }
      }
    }
    user.map(u => Event(u, value))
  }

  def main(args: Array[String]): Unit = {
    val conf = parseArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source: KafkaSource[String] = KafkaSource.builder[String]()
      .setBootstrapServers(conf.bootstrap)
      .setTopics(conf.sourceTopic)
      .setGroupId("flink-local-aggregation")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
      .build()

    val sink: KafkaSink[String] = KafkaSink.builder[String]()
      .setBootstrapServers(conf.bootstrap)
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder[String]()
          .setTopic(conf.sinkTopic)
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .build()

    // Much simpler: parse JSON, group by user, sum values in tumbling windows
    val aggregated = env
      .fromSource(source, WatermarkStrategy.noWatermarks[String](), "kafka-source")
      .flatMap((line: String, out: Collector[Event]) => parseJsonLine(line).foreach(out.collect))
      .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(classOf[Event]))
      .keyBy(_.user)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(conf.windowSec)))
      .reduce((a, b) => Event(a.user, a.value + b.value))
      .map(e => s"${e.user},${e.value}")

    aggregated.sinkTo(sink)

    env.execute("simple-kafka-aggregation")
  }

  private def parseArgs(args: Array[String]): Conf = {
    var c = Conf()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--bootstrap" => c = c.copy(bootstrap = args(i+1)); i += 2
        case "--source"    => c = c.copy(sourceTopic = args(i+1)); i += 2
        case "--sink"      => c = c.copy(sinkTopic = args(i+1)); i += 2
        case "--windowSec" => c = c.copy(windowSec = args(i+1).toInt); i += 2
        case other => throw new IllegalArgumentException(s"Unknown arg: $other")
      }
    }
    c
  }
}


