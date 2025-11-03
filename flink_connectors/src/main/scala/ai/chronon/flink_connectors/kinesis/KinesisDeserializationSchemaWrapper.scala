package ai.chronon.flink_connectors.kinesis

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema
import org.apache.flink.util.Collector

import java.util

/** Adapter that bridges the API mismatch between FlinkKinesisConsumer and Chronon's DeserializationSchema.
  * 
  * FlinkKinesisConsumer expects a method that returns a single value (T), but Chronon's schemas
  * use the Collector-based API that can emit 0+ records per input. This wrapper creates a local
  * collector to capture results and returns only the first one.
  * 
  * **Limitation**: If the underlying schema emits multiple records, only the first is returned.
  * This is acceptable for typical use cases (CDC events with null 'before', single events per record)
  * but would cause data loss for multi-record scenarios. For full multi-record support, implement
  * a custom source similar to PubSubSource.
  *
  * @param deserializationSchema The Chronon DeserializationSchema to wrap
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
    val results = new util.ArrayList[T]()
    val collector = new Collector[T] {
      override def collect(record: T): Unit = results.add(record)
      override def close(): Unit = {}
    }

    deserializationSchema.deserialize(recordValue, collector)
    
    if (!results.isEmpty) results.get(0) else null.asInstanceOf[T]
  }

  override def getProducedType: TypeInformation[T] = deserializationSchema.getProducedType
}

