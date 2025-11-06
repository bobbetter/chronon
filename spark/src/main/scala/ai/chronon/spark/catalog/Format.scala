package ai.chronon.spark.catalog

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.function

object TableCache {
  private val dfMap: ConcurrentMap[String, DataFrame] = new ConcurrentHashMap[String, DataFrame]()

  def get(tableName: String)(implicit sparkSession: SparkSession): DataFrame = {
    dfMap.computeIfAbsent(tableName,
                          new function.Function[String, DataFrame] {
                            override def apply(t: String): DataFrame = {
                              sparkSession.read.table(t)
                            }
                          })
  }

  def remove(tableName: String): Unit = {
    dfMap.remove(tableName)
  }
}

trait Format {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def tableProperties: Map[String, String] = Map.empty[String, String]

  def table(tableName: String, partitionFilters: String, cacheDf: Boolean = false)(implicit
      sparkSession: SparkSession): DataFrame = {

    val df = if (cacheDf) {
      TableCache.get(tableName)
    } else {
      sparkSession.read.table(tableName)
    }

    if (partitionFilters.isEmpty) {
      df
    } else {
      df.where(partitionFilters)
    }

  }

  // Return the primary partitions (based on the 'partitionColumn') filtered down by sub-partition filters if provided
  // If subpartition filters are supplied and the format doesn't support it, we throw an error
  def primaryPartitions(tableName: String,
                        partitionColumn: String,
                        partitionFilters: String,
                        subPartitionsFilter: Map[String, String] = Map.empty)(implicit
      sparkSession: SparkSession): List[String] = {

    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    val partitionSeq = partitions(tableName, partitionFilters)(sparkSession)

    partitionSeq.flatMap { partitionMap =>
      if (
        subPartitionsFilter.forall { case (k, v) =>
          partitionMap.get(k).contains(v)
        }
      ) {
        partitionMap.get(partitionColumn)
      } else {
        None
      }
    }
  }

  // Return a sequence for partitions where each partition entry consists of a map of partition keys to values
  // e.g. Seq(
  //         Map("ds" -> "2023-04-01", "hr" -> "12"),
  //         Map("ds" -> "2023-04-01", "hr" -> "13")
  //         Map("ds" -> "2023-04-02", "hr" -> "00")
  //      )
  def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]]

  // Does this format support sub partitions filters
  def supportSubPartitionsFilter: Boolean

}

object Format {

  def parseHiveStylePartition(pstring: String): List[(String, String)] = {
    pstring
      .split("/")
      .map { part =>
        val p = part.split("=", 2)
        p(0) -> p(1)
      }
      .toList
  }

  def getCatalog(tableName: String)(implicit sparkSession: SparkSession): String = {
    val parsed = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val parsedCatalog = parsed.toList match {
      case catalog :: namespace :: tableName :: Nil => catalog
      case namespace :: tableName :: Nil            => sparkSession.catalog.currentCatalog()
      case tableName :: Nil                         => sparkSession.catalog.currentCatalog()
      case _ => throw new IllegalStateException(s"Invalid table naming convention specified: ${tableName}")
    }
    parsedCatalog
  }

}
