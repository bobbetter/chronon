/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online

import ai.chronon.aggregator.base.BottomK
import ai.chronon.aggregator.stats.EditDistance
import ai.chronon.api.UnknownType
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util
import java.util.Properties
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.asScalaIteratorConverter

object TopicChecker {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def getPartitions(topic: String, bootstrap: String, additionalProps: Map[String, String] = Map.empty): Int = {
    val props = mapToJavaProperties(additionalProps ++ Map(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap))
    val adminClient = AdminClient.create(props)
    val topicDescription = adminClient.describeTopics(util.Arrays.asList(topic)).values().get(topic);
    topicDescription.get().partitions().size()
  }

  def topicShouldExist(topic: String, bootstrap: String, additionalProps: Map[String, String] = Map.empty): Unit = {
    val props = mapToJavaProperties(additionalProps ++ Map(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap))
    try {
      val adminClient = AdminClient.create(props)
      val options = new ListTopicsOptions()
      options.listInternal(true)
      val topicsList = adminClient.listTopics(options)
      val topicsResult = topicsList.namesToListings().get()
      if (!topicsResult.containsKey(topic)) {
        val closestK = new BottomK[(Double, String)](UnknownType(), 5)
        val result = new util.ArrayList[(Double, String)]()
        topicsResult // find closestK matches based on edit distance.
          .entrySet()
          .iterator()
          .asScala
          .map { topicListing =>
            val existing = topicListing.getValue.name()
            EditDistance.betweenStrings(existing, topic).total / existing.length.toDouble -> existing
          }
          .foldLeft(result)((cnt, elem) => closestK.update(cnt, elem))
        closestK.finalize(result)
        throw new RuntimeException(s"""
                                      |Requested topic: $topic is not found in broker: $bootstrap.
                                      |Either the bootstrap is incorrect or the topic is. 
                                      |
                                      | ------ Most similar topics are ------
                                      |
                                      |  ${result.asScala.map(_._2).mkString("\n  ")}
                                      |
                                      | ------ End ------
                                      |""".stripMargin)
      } else {
        logger.info(s"Found topic $topic in bootstrap $bootstrap.")
      }
    } catch {
      case ex: Exception => throw new RuntimeException(s"Failed to check for topic ${topic} in ${bootstrap}", ex)
    }
  }

  def mapToJavaProperties(map: Map[String, String]): Properties = {
    val props = new Properties()
    map.foreach { case (k, v) => props.put(k, v) }
    props
  }
}
