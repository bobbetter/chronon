package ai.chronon.spark.interactive

import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Extensions.StringOps
import ai.chronon.online.CatalystUtil
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Paths
import scala.collection.mutable

// expects a directory of files in the following format
//   <namespace>/<table>/*.parquet
//
// if warehouseDirectory is not set, we will look for `$CHRONON_ROOT/local_warehouse/`
// if CHRONON_ROOT is not set, we will look for ./local_warehouse/ (current working directory basically)
class LocalWarehouse(warehouseDirectory: Option[String]) {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  private val spark: SparkSession = CatalystUtil.session

  // configure logging should be always called after spark session is built
  // or spark will override
  SparkSessionBuilder.configureLogging()

  private val tu: TableUtils = TableUtils(spark)

  private val tables: Set[String] = registerTables()

  def existingTables: Set[String] = tables

  private lazy val warehouseDir: String = {

    def addSuffix(dir: Option[String]): Option[String] =
      dir.map(d => Paths.get(d, "local_warehouse").toAbsolutePath.toString)

    val chrononRoot = addSuffix(sys.env.get("CHRONON_ROOT"))
    val currentDir = addSuffix(Option(System.getProperty("user.dir")))

    val chosen = warehouseDirectory.orElse(chrononRoot).orElse(currentDir)

    logger.info(s"""
         |    warehouse constructor arg : $warehouseDirectory
         |       chronon root warehouse : $chrononRoot
         |  working directory warehouse : $currentDir
         |             chosen warehouse : ${chosen.map(_.green)}
         |""".stripMargin)

    chosen.get
  }

  private val existingFunctionRegistrations: mutable.Set[String] = mutable.Set.empty

  def runSql(query: String): DataFrame = tu.sql(query)

  def runSetup(setup: String): Unit = {

    if (existingFunctionRegistrations.contains(setup)) {
      logger.info(s"Function has been already registered with statement [$setup]. Not evaluating again.")
      return
    }

    tu.sql(setup)

    if (setup.contains("CREATE FUNCTION")) {
      existingFunctionRegistrations.add(setup)
    }
  }

  // registers tables inside the warehouse dir
  private def registerTables(): Set[String] = {

    logger.info(s"Using local-warehouse from $warehouseDir")

    val rootDir = new File(warehouseDir)

    require(rootDir.exists(), s"Warehouse directory not found: $warehouseDirectory")
    require(rootDir.isDirectory, "Warehouse directory is not a string")

    val (namespaceDirs, ignored) = rootDir.listFiles().partition(_.isDirectory)

    if (ignored.nonEmpty)
      logger.warn("Ignoring files at the same level as namespace dirs:\n  " + ignored.map(_.getName).mkString("\n  "))

    namespaceDirs.flatMap { namespaceDir =>
      val namespace = namespaceDir.getName
      val (tableDirs, ignored) = namespaceDir.listFiles().partition(_.isDirectory)

      if (ignored.nonEmpty)
        logger.warn("Ignoring files at the same level as table dir:\n  " + ignored.map(_.getName).mkString("\n  "))

      tableDirs.map { tableDir =>
        val table = tableDir.getName

        val parquetPaths = tableDir
          .listFiles()
          .filter(_.isFile)
          .filter(_.getName.endsWith(".parquet"))
          .map(_.getAbsolutePath)

        val qualifiedTable = s"$namespace.$table".sanitize

        logger.info(s"Registering table $qualifiedTable with ${parquetPaths.length} parquet files.")

        spark.read
          .parquet(parquetPaths: _*)
          .createOrReplaceTempView(qualifiedTable)

        // cache the table on first read in memory - default ser is memory_and_disk
        spark.sql(s"CACHE LAZY TABLE ${qualifiedTable}")

        qualifiedTable
      }
    }.toSet

  }

}
