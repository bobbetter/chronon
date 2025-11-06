package ai.chronon.flink.validation

import org.apache.commons.lang3.builder.EqualsBuilder

import scala.collection.immutable.SortedMap
import scala.collection.mutable

case class ComparisonResult(recordId: String,
                            isMatch: Boolean,
                            catalystResult: Seq[Map[String, Any]],
                            sparkDfResult: Seq[Map[String, Any]],
                            differences: Map[String, (Any, Any)]) {
  override def toString: String = {
    s"""
           |RecordId: $recordId
           |Is Match: $isMatch
           |Catalyst Result: $catalystResult
           |Spark DF Result: $sparkDfResult
           |Differences (diff_type -> (catalystValue, sparkDfValue) ) : $differences
           |""".stripMargin
  }
}

object SparkExprEvalComparisonFn {

  /** Utility function to compare the results of Catalyst and Spark DataFrame evaluation
    * for a given recordId.
    * At a high level comparison is done as follows:
    * 1. If the number of rows in the catalyst vs spark df result is different, the results are considered different ("result_count" -> (catalystSize, sparkDfSize))
    * 2. As the rows in the result can be in any order (which is ok from a Catalyst perspective), we sort the rows prior to comparing.
    * 3. For each row, we compare the key-value pairs in the maps.
    * If the size of the maps is different, the results are considered different ("result_row_size_$i" -> (catalystSize, sparkDfSize))
    * If the values are different, the results are considered different ("result_row_value_${i}_$k" -> (catalystValue, sparkDfValue))
    */
  private[validation] def compareResultRows(recordId: String,
                                            catalystResult: Seq[Map[String, Any]],
                                            sparkDfResult: Seq[Map[String, Any]]): ComparisonResult = {
    if (catalystResult.size != sparkDfResult.size) {
      return ComparisonResult(
        recordId = recordId,
        isMatch = false,
        catalystResult = catalystResult,
        sparkDfResult = sparkDfResult,
        differences = Map("result_count" -> (catalystResult.size, sparkDfResult.size))
      )
    }

    // We can expect multiple rows in the result (e.g. for explode queries) and these rows
    // might be ordered differently. We need to compare the rows in a way that is order-agnostic.
    val sortedCatalystResult = catalystResult.map(m => SortedMap[String, Any]() ++ m).sortBy(_.toString)
    val sortedSparkDfResult = sparkDfResult.map(m => SortedMap[String, Any]() ++ m).sortBy(_.toString)
    // Compare each pair of maps
    val differences = mutable.Map[String, (Any, Any)]()

    for (i <- sortedCatalystResult.indices) {
      val map1 = sortedCatalystResult(i)
      val map2 = sortedSparkDfResult(i)

      if (map1.size != map2.size) {
        differences += s"result_row_size_$i" -> (map1.size, map2.size)
      } else {
        map1.foreach { case (k, v1) =>
          val v2 = map2.getOrElse(k, null)

          if (!deepEquals(v1, v2)) {
            differences += s"result_row_value_${i}_$k" -> (v1, v2)
          }
        }
      }
    }

    if (differences.isEmpty) {
      ComparisonResult(
        recordId = recordId,
        isMatch = true,
        catalystResult = catalystResult,
        sparkDfResult = sparkDfResult,
        differences = Map.empty
      )
    } else {
      ComparisonResult(
        recordId = recordId,
        isMatch = false,
        catalystResult = catalystResult,
        sparkDfResult = sparkDfResult,
        differences = differences.toMap
      )
    }
  }

  // Helper method for deep equality - primarily used to special case types like Maps that don't match correctly
  // in EqualsBuilder.reflectionEquals across scala versions 2.12 and 2.13.
  private def deepEquals(a: Any, b: Any): Boolean = (a, b) match {
    case (null, null)          => true
    case (null, _) | (_, null) => false
    case (a: Map[_, _], b: Map[_, _]) =>
      a.size == b.size && a.asInstanceOf[Map[Any, Any]].forall { case (k, v) =>
        b.asInstanceOf[Map[Any, Any]].get(k) match {
          case Some(bValue) => deepEquals(v, bValue)
          case None         => false
        }
      }
    case _ => EqualsBuilder.reflectionEquals(a, b)
  }
}
