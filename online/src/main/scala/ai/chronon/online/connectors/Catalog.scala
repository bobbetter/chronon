package ai.chronon.online.connectors

import ai.chronon.api.DataSpec
import ai.chronon.online.KVStore

import scala.concurrent.duration.Duration

case class Table(databaseName: String, tableName: String) extends CatalogEntry {
  override def toKey: String = {
    val dbTable = (Option(databaseName) ++ Option(tableName)).mkString("/")
    s"table/$dbTable"
  }
}

case class Topic(name: String, cluster: String) extends CatalogEntry {
  override def toKey: String = {
    val topicCluster = (Option(cluster) ++ Option(name)).mkString("/")
    s"topic/$topicCluster"
  }
}

// catalog entry types need a key to be stored in the KV store
trait CatalogEntry {
  def toKey: String
}

abstract class Catalog(kvStore: KVStore) {
  def putSpec(element: CatalogEntry, spec: DataSpec): Unit = thriftKvUtil.put("spec", element.toKey, spec)
  def getSpec(element: CatalogEntry): DataSpec = thriftKvUtil.get("spec", element.toKey)

  private val CatalogKvTableName = "zipline_catalog"
  private val WaitTime = Duration.Inf
  private val thriftKvUtil = new ThriftKvUtil(kvStore, CatalogKvTableName, WaitTime)
}
