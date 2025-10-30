package ai.chronon.flink_connectors.kinesis

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema
import org.apache.flink.util.Collector

import java.util

/** Wrapper around a Flink DeserializationSchema to allow it to work with Kinesis consumer.
  * This wrapper handles the case where the underlying schema uses Collector-based deserialization
  * (which produces multiple output records per input record) by collecting results into a list
  * and returning them.
  *
  * @param deserializationSchema The Flink DeserializationSchema to wrap
  * @tparam T The type of elements produced
  */
class KinesisDeserializationSchemaWrapper[T](deserializationSchema: DeserializationSchema[T])
    extends KinesisDeserializationSchema[T] {

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    deserializationSchema.open(context)
  }

  override def deserialize(
      recordValue: Array[Byte],
      partitionKey: String,
      seqNum: String,
      approxArrivalTimestamp: Long,
      stream: String,
      shardId: String
  ): T = {
    // Collect results into a list - for schemas that use the Collector API
    val results = new util.ArrayList[T]()
    val collector = new Collector[T] {
      override def collect(record: T): Unit = results.add(record)
      override def close(): Unit = {}
    }

    try {
      // Try the Collector-based deserialization first
      deserializationSchema.deserialize(recordValue, collector)
      
      // Return the first result, or null if none
      // Note: Kinesis consumer expects one record per call, so if there are multiple
      // results, only the first is returned. For full support of multi-record outputs,
      // a flatMap operator should be used downstream.
      if (results.size() > 0) results.get(0) else null.asInstanceOf[T]
    } catch {
      case _: UnsupportedOperationException =>
        // Fall back to simple deserialization if Collector-based is not supported
        deserializationSchema.deserialize(recordValue)
    }
  }

  override def getProducedType: TypeInformation[T] = deserializationSchema.getProducedType
}

