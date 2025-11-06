package ai.chronon.spark.stats.drift

import ai.chronon.api.Constants
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.observability.Cardinality
import ai.chronon.observability.TileSummaryKey
import ai.chronon.observability.TileSummary
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.types
import org.apache.spark.sql.types.StructType

import java.lang
import scala.collection.mutable

object Expressions {

  def isScalar(dataType: types.DataType): Boolean =
    dataType match {
      case types.StringType | types.ShortType | types.BooleanType | types.IntegerType | types.LongType |
          types.FloatType | types.DoubleType =>
        true
      case _ => false
    }

  /* ---------------------- NOTATION ----------------- */
  // one of those cases where we prioritize readability over idiomatic scala.
  // I think, code would be harder to read without short-form & abbreviated notation

  private object Agg {
    private val percentileBreaks: String = (0 to 20).map(_ * 0.05).map(num => f"$num%.2f").toArray.mkString(", ")
    // aggregation expressions - we will substitute out the "col" downstream
    val hist = "count_histogram(_col_)"
    val ptile: String = s"approx_percentile(_col_, array($percentileBreaks))"
    val arrPtile = "array_percentile(_col_)"
    val arrHist = "array_histogram(_col_)"
    val arrNulls = "sum(aggregate(_col_, 0, (acc, v) -> if(v is NULL, acc + 1, acc)))"
    val arrCount = "sum(size(_col_))"

    val uniq = "approx_count_distinct(_col_)"
    val arrDblUniq = "array_dbl_distinct(_col_)"
    val arrStrUniq = "array_str_distinct(_col_)"
    val count = "count(_col_)"
    val nullCount = "sum(if(_col_ is NULL, 1, 0))"
  }

  // expressions over raw columns
  private object Inp {
    val len = "length(_col_)"
    val cLen = "size(_col_)"
    val strCast = "CAST(_col_ as STRING)"
    val dblCast = "CAST(_col_ as DOUBLE)"
    val arrStrCast = "transform(_col_, x -> CAST(x as STRING))"
    val arrDblCast = "transform(_col_, x -> CAST(x as Double))"
    val mapVals = "map_values(_col_)"
    val lenVals = "CAST(transform(map_values(_col_), x -> length(x)) AS FLOAT)"
    val mapStrCast = "transform(map_values(_col_), x -> CAST(x as STRING))"
    val mapDblCast = "transform(map_values(_col_), x -> CAST(x as DOUBLE))"
  }

  private object MetricName {
    val nullCount = "null_count"
    val count = "count"
    val histogram = "histogram"
    val percentiles = "percentiles"
    val innerNullCount = "inner_null_count"
    val innerCount = "inner_count"
    val lengthPercentiles = "length_percentiles"
    val stringLengthPercentiles = "string_length_percentiles"
  }

  case class TileRow(partition: String, tileTs: Long, key: TileSummaryKey, summaries: TileSummary)
  def summaryPopulatorFunc(summaryExpressions: Seq[SummaryExpression],
                           summarySchema: StructType,
                           keyBuilder: (String, sql.Row) => TileSummaryKey,
                           partitionColumn: String): Row => Seq[TileRow] = {
    val funcs = summaryExpressions.map { se =>
      val metricName = se.name
      val colName = se.column
      val populatorFunc = metricPopulatorFunc(colName, metricName, summarySchema)
      (row: Row, columnTileSummaries: mutable.Map[String, TileSummary]) =>
        populatorFunc(row, columnTileSummaries.getOrElseUpdate(colName, new TileSummary()))
    }

    val partitionIndex = summarySchema.fieldIndex(partitionColumn)
    val tileIndex = summarySchema.fieldIndex(Constants.TileColumn)

    { row: Row =>
      val columnTileSummaries: mutable.Map[String, TileSummary] = mutable.Map.empty
      funcs.foreach(_(row, columnTileSummaries))
      val tileTimestamp = row.getLong(tileIndex)
      val partition = row.getString(partitionIndex)
      columnTileSummaries.iterator.map { case (colName, tileSummaries) =>
        TileRow(partition, tileTimestamp, keyBuilder(colName, row), tileSummaries)
      }.toSeq
    }
  }

  // creates a function to populate TileSummaries object given schema and names
  private def metricPopulatorFunc(colName: String,
                                  metricName: String,
                                  schema: StructType): (Row, TileSummary) => Unit = {
    val summaryCol = s"${colName}_${metricName}"
    assert(schema.fieldNames.contains(summaryCol),
           s"Summary column ${summaryCol} not found among: ${schema.fieldNames.mkString(", ")}")
    val index = schema.fieldIndex(summaryCol)

    metricName match {
      case MetricName.nullCount => (row: Row, summaries: TileSummary) => summaries.setNullCount(row.getLong(index))
      case MetricName.count     => (row: Row, summaries: TileSummary) => summaries.setCount(row.getLong(index))
      case MetricName.histogram =>
        (row: Row, summaries: TileSummary) =>
          summaries.setHistogram(
            row
              .getMap[String, Long](index)
              .mapValues(lang.Long.valueOf)
              .toMap
              .toJava
          )
      case MetricName.percentiles =>
        (row: Row, summaries: TileSummary) =>
          if (row.isNullAt(index)) {
            summaries.setPercentiles(null)
          } else {
            summaries.setPercentiles(row.getSeq[Double](index).map(lang.Double.valueOf).toJava)
          }

      case MetricName.innerNullCount =>
        (row: Row, summaries: TileSummary) => summaries.setInnerNullCount(row.getLong(index))

      case MetricName.innerCount =>
        (row: Row, summaries: TileSummary) => summaries.setInnerCount(row.getLong(index))

      case MetricName.lengthPercentiles =>
        (row: Row, summaries: TileSummary) =>
          summaries.setLengthPercentiles(row.getSeq[Int](index).map(lang.Integer.valueOf).toJava)

      case MetricName.stringLengthPercentiles =>
        (row: Row, summaries: TileSummary) =>
          summaries.setStringLengthPercentiles(row.getSeq[Int](index).map(lang.Integer.valueOf).toJava)
    }
  }

  case class CardinalityExpression(select: Option[String], aggExpr: String) {
    def render(col: String): CardinalityExpression = {
      val aggregatedColumnName = s"${col}_cardinality"
      val inputColName = s"${aggregatedColumnName}_input"
      CardinalityExpression(
        Some(s"${select.map(_.replace("_col_", col)).getOrElse(col)} as `$inputColName`"),
        s"${aggExpr.replace("_col_", inputColName)} as `$aggregatedColumnName`"
      )
    }
  }

  object CardinalityExpression {
    private def ce(select: String, aggExpr: String): CardinalityExpression =
      CardinalityExpression(Option(select), aggExpr)

    def apply(dataType: types.DataType): CardinalityExpression = {
      dataType match {
        case dType if isScalar(dType) => ce(null, Agg.uniq)
        case types.ArrayType(elemType, _) =>
          elemType match { // histogram
            case types.StringType         => ce(null, Agg.arrStrUniq)
            case types.DoubleType         => ce(null, Agg.arrDblUniq)
            case eType if isScalar(eType) => ce(Inp.arrDblCast, Agg.arrDblUniq)
            case _ => throw new UnsupportedOperationException(s"Unsupported array element type $elemType")
          }
        // TODO: measure and handle map key cardinality
        case types.MapType(_, vType, _) =>
          vType match {
            case types.StringType         => ce(Inp.mapVals, Agg.arrStrUniq)
            case types.DoubleType         => ce(Inp.mapVals, Agg.arrDblUniq)
            case eType if isScalar(eType) => ce(Inp.mapDblCast, Agg.arrDblUniq)
            case _ => throw new UnsupportedOperationException(s"Unsupported map value type $vType")
          }
        case _ => throw new UnsupportedOperationException(s"Unsupported data type $dataType")
      }
    }
  }

  case class SummaryExpression(mapEx: String, aggEx: String, name: String, column: String) extends Serializable {
    val aggregatedColumnName: String = s"${column}_$name"
    val inputColName: String = s"${aggregatedColumnName}_input"
    val inputAlias: String = s"$mapEx as `$inputColName`"
    val aggAlias: String = s"$aggEx as `$aggregatedColumnName`"
  }

  object SummaryExpression {
    def apply(mapEx: Option[String], aggEx: String, name: String, column: String): SummaryExpression = {
      val aggregatedColumnName = s"${column}_${name}"
      val inputColName = s"${aggregatedColumnName}_input"
      val mapExStr = mapEx.map(_.replace("_col_", column)).getOrElse(column)
      val aggExStr = aggEx.replace("_col_", inputColName)
      SummaryExpression(mapExStr, aggExStr, name, column)
    }

    def of(dataType: types.DataType, cardinality: Cardinality, column: String): Seq[SummaryExpression] = {
      def se(mapEx: String, aggFunc: String, name: String): Seq[SummaryExpression] =
        Seq(SummaryExpression(Option(mapEx), aggFunc, name, column))

      val specificExpressions = cardinality match {
        case Cardinality.LOW =>
          dataType match {
            case types.StringType         => se(null, Agg.hist, MetricName.histogram)
            case dType if isScalar(dType) => se(Inp.strCast, Agg.hist, MetricName.histogram)
            case types.ArrayType(elemType, _) =>
              se(Inp.cLen, Agg.ptile, MetricName.lengthPercentiles) ++
                se(null, Agg.arrNulls, MetricName.innerNullCount) ++
                se(null, Agg.arrCount, MetricName.innerCount) ++
                (elemType match { // histogram
                  case types.StringType         => se(null, Agg.arrHist, MetricName.histogram)
                  case eType if isScalar(eType) => se(Inp.arrStrCast, Agg.arrHist, MetricName.histogram)
                  case _                        => Seq.empty
                })

            // TODO: deal with map keys - as histogram - high cardinality keys vs low cardinality?
            // TODO: frequent key - top_k via approx_histogram
            case types.MapType(_, vType, _) =>
              se(Inp.cLen, Agg.ptile, MetricName.lengthPercentiles) ++ // length drift
                se(Inp.mapVals, Agg.arrNulls, MetricName.innerNullCount) ++
                (vType match { // histogram of values
                  case types.StringType         => se(Inp.mapVals, Agg.arrHist, MetricName.histogram)
                  case eType if isScalar(eType) => se(Inp.mapStrCast, Agg.arrHist, MetricName.histogram)
                  case _                        => Seq.empty
                })
            case _ => throw new UnsupportedOperationException(s"Unsupported data type $dataType")
          }

        // map[string::column, map[string::metric_name, value]]
        case Cardinality.HIGH =>
          dataType match {
            case types.StringType         => se(Inp.len, Agg.ptile, MetricName.stringLengthPercentiles)
            case dType if isScalar(dType) => se(Inp.dblCast, Agg.ptile, MetricName.percentiles)
            case types.ArrayType(elemType, _) =>
              se(null, Agg.arrNulls, MetricName.innerNullCount) ++
                se(null, Agg.arrCount, MetricName.innerCount) ++
                se(Inp.cLen, Agg.ptile, MetricName.lengthPercentiles) ++ (elemType match {
                  case types.StringType         => se(Inp.len, Agg.ptile, MetricName.lengthPercentiles)
                  case eType if isScalar(eType) => se(Inp.arrDblCast, Agg.arrPtile, MetricName.percentiles)
                  case _                        => Seq.empty
                })
            case types.MapType(_, vType, _) =>
              se(Inp.mapVals, Agg.arrNulls, MetricName.innerNullCount) ++
                se(Inp.mapVals, Agg.arrCount, MetricName.innerCount) ++
                se(Inp.cLen, Agg.ptile, MetricName.lengthPercentiles) ++ (vType match {
                  case types.StringType         => se(Inp.lenVals, Agg.arrPtile, MetricName.stringLengthPercentiles)
                  case eType if isScalar(eType) => se(Inp.mapDblCast, Agg.arrPtile, MetricName.percentiles)
                  case _                        => Seq.empty
                })
            case _ => throw new UnsupportedOperationException(s"Unsupported data type $dataType")
          }
      }
      val counts = se(null, Agg.count, MetricName.count)
      val nullCounts = se(null, Agg.nullCount, MetricName.nullCount)
      specificExpressions ++ counts ++ nullCounts
    }
  }
}
