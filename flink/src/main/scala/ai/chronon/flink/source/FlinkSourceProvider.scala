package ai.chronon.flink.source

import ai.chronon.online.TopicInfo
import org.apache.flink.api.common.serialization.DeserializationSchema

object FlinkSourceProvider {
  def build[T](props: Map[String, String],
               deserializationSchema: DeserializationSchema[T],
               topicInfo: TopicInfo): FlinkSource[T] = {
    topicInfo.messageBus match {
      case "kafka" =>
        new KafkaFlinkSource(props, deserializationSchema, topicInfo)
      case "pubsub" =>
        loadPubsubSource(props, deserializationSchema, topicInfo)
      case "kinesis" =>
        loadKinesisSource(props, deserializationSchema, topicInfo)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported message bus: ${topicInfo.messageBus}")
    }
  }

  // Pubsub source is loaded via reflection as we don't want the Flink module to depend on the PubSub connector
  // module as we don't want to pull in Gcp deps in contexts such as running in Aws
  private def loadPubsubSource[T](props: Map[String, String],
                                  deserializationSchema: DeserializationSchema[T],
                                  topicInfo: TopicInfo): FlinkSource[T] = {
    val cl = Thread.currentThread().getContextClassLoader // Use Flink's classloader
    val cls = cl.loadClass("ai.chronon.flink_connectors.pubsub.PubSubFlinkSource")
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(props, deserializationSchema, topicInfo)
    onlineImpl.asInstanceOf[FlinkSource[T]]
  }

  private def loadKinesisSource[T](props: Map[String, String],
                                   deserializationSchema: DeserializationSchema[T],
                                   topicInfo: TopicInfo): FlinkSource[T] = {
    val cl = Thread.currentThread().getContextClassLoader
    val cls = cl.loadClass("ai.chronon.flink_connectors.kinesis.KinesisFlinkSource")
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(props, deserializationSchema, topicInfo)
    onlineImpl.asInstanceOf[FlinkSource[T]]
  }
}
