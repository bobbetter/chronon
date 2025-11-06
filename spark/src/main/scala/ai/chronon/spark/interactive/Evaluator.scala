package ai.chronon.spark.interactive

import ai.chronon.api
import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.QueryUtils
import ai.chronon.api.QueryUtils.SourceSqlBundle
import ai.chronon.api.ThriftJsonCodec
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import py4j.GatewayServer

class Evaluator(localWarehouse: LocalWarehouse) {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  case class EvaluationResult(malformedQuery: String, df: DataFrame)

  def evalSource(conf: String, limit: Int = 5): EvaluationResult = {

    val source = ThriftJsonCodec.fromJson[api.Source](conf, check = false)
    val sourceSqlBundle = QueryUtils.sqlBundle(source, sanitize = true)

    try {

      EvaluationResult(null, runSourceBundle(sourceSqlBundle).limit(limit))

    } catch {

      case analysisException: AnalysisException =>
        EvaluationResult(analysisException.getMessage, null)

    }
  }

  def evalQuery(query: String, limit: Int = 5): EvaluationResult = {

    try {

      EvaluationResult(null, localWarehouse.runSql(query).limit(limit))

    } catch {

      case analysisException: AnalysisException =>
        EvaluationResult(analysisException.getMessage, null)

    }
  }

  private def runSourceBundle(sourceSqlBundle: SourceSqlBundle): DataFrame = {

    val missingTables = sourceSqlBundle.tables -- localWarehouse.existingTables

    require(missingTables.isEmpty, "Missing tables in local warehouse: " + missingTables.mkString("[", ", ", "]"))

    Option(sourceSqlBundle.setups).foreach(_.foreach { setup =>
      logger.info(s"Running setup query: $setup")
      localWarehouse.runSetup(setup)
    })

    logger.info(s"""Running query from source:
         |
         |${sourceSqlBundle.scanQuery.green}
         |
         |""".stripMargin)

    localWarehouse.runSql(sourceSqlBundle.scanQuery)
  }

}

object Evaluator extends App {

  private val warehouse = new LocalWarehouse(None)
  private val evaluator = new Evaluator(warehouse)
  private val gateway = new GatewayServer(evaluator)

  gateway.start()
  println("Gateway Server Started")

}
