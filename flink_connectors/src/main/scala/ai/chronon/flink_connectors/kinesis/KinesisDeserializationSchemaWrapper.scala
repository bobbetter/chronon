package ai.chronon.flink_connectors.kinesis

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema
import org.apache.flink.util.Collector

import java.util

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
