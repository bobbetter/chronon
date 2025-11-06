package ai.chronon.spark.streaming

import ai.chronon.api
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.online.TopicChecker.getPartitions
import ai.chronon.online.TopicChecker.logger
import ai.chronon.spark.Driver
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object TopicCheckerApp {
  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: ScallopOption[String] = opt[String](descr = "Conf to pull topic and bootstrap server information")
    val bootstrap: ScallopOption[String] = opt[String](descr = "Kafka bootstrap server in host:port format")
    val topic: ScallopOption[String] = opt[String](descr = "kafka topic to check metadata for")
    verify()
  }

  // print out number of partitions and exit
  def main(argSeq: Array[String]): Unit = {
    val args = new Args(argSeq)
    val (topic, bootstrap) = if (args.conf.isDefined) {
      val confPath = args.conf()
      val groupBy = Driver.parseConf[api.GroupBy](confPath)
      val source = groupBy.streamingSource.get
      val topic = source.cleanTopic
      val tokens = source.topicTokens
      lazy val host = tokens.get("host")
      lazy val port = tokens.get("port")
      lazy val hostPort = s"${host.get}:${port.get}"
      topic -> args.bootstrap.getOrElse(hostPort)
    } else {
      args.topic() -> args.bootstrap()
    }
    logger.info(getPartitions(topic, bootstrap).toString)
    System.exit(0)
  }
}
