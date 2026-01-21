package ai.chronon.flink.aws

import ai.chronon.flink.FlinkJob
import org.slf4j.LoggerFactory

import java.util.{Map => JMap}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

/** AWS Managed Apache Flink wrapper for Chronon FlinkJob.
  *
  * This wrapper bridges the gap between:
  * - AWS Managed Flink's ParameterTool.fromApplicationProperties() configuration method
  * - FlinkJob's Scallop CLI-based argument parsing
  *
  * AWS Managed Flink blocks System.exit() calls for security, and Scallop calls System.exit()
  * when required arguments are missing. Since AWS Managed Flink doesn't pass CLI arguments
  * (it uses application properties instead), this wrapper reads those properties and converts
  * them to CLI-style arguments before calling FlinkJob.main().
  *
  * Usage:
  * 1. Set this class as the main class: ai.chronon.flink.aws.AwsFlinkJobWrapper
  * 2. Configure application properties in AWS Managed Flink console under "FlinkApplicationProperties"
  *
  * Required Properties (in FlinkApplicationProperties property group):
  *   - groupby-name: The name of the groupBy to process (e.g., "mli.logins.v1__1")
  *   - online-class: Fully qualified Online.Api class (e.g., "ai.chronon.integrations.aws.AwsApiImpl")
  *   - streaming-manifest-path: S3 bucket path for manifest files
  *
  * Optional Properties:
  *   - kinesis-stream: Kinesis stream name (for Kinesis sources)
  *   - kafka-bootstrap: Kafka bootstrap server (for Kafka sources)
  *   - parent-job-id: Parent job ID for tracking
  *   - validate: Run in validation mode (true/false)
  *   - validate-rows: Number of rows for validation
  *   - enable-debug: Enable debug logging (true/false)
  *   - mock-source: Use mock data source (true/false)
  *   - ztasks: Number of tasks to pass as -Ztasks=<value>
  *
  * API Properties (passed as -Z flags):
  *   Any property starting with "api." will be converted to -ZKEY=VALUE format.
  *   Example: api.DYNAMO_ENDPOINT=http://dynamodb.us-east-2.amazonaws.com
  *            becomes: -ZDYNAMO_ENDPOINT=http://dynamodb.us-east-2.amazonaws.com
  */
object AwsFlinkJobWrapper {

  private val logger = LoggerFactory.getLogger(getClass)

  // Map of AWS application property names to Scallop CLI argument names
  // ONLY include arguments that FlinkJob's Scallop config actually accepts!
  // See FlinkJob.scala JobArgs class for the list of valid arguments.
  // NOTE: kinesis-stream is NOT a valid FlinkJob argument - it's read from GroupBy metadata
  private val PropertyToArgMapping = Map(
    "groupby-name" -> "--groupby-name",
    "online-class" -> "--online-class",
    "streaming-manifest-path" -> "--streaming-manifest-path",
    "kafka-bootstrap" -> "--kafka-bootstrap",
    "parent-job-id" -> "--parent-job-id",
    "validate" -> "--validate",
    "validate-rows" -> "--validate-rows",
    "enable-debug" -> "--enable-debug",
    "mock-source" -> "--mock-source"
    // kinesis-stream is intentionally NOT included - it's not a FlinkJob CLI argument
  )

  // Properties that are boolean flags (don't need values)
  private val BooleanFlags = Set("validate", "enable-debug", "mock-source")

  def main(args: Array[String]): Unit = {
    logger.info("AwsFlinkJobWrapper starting...")
    logger.info(s"Original CLI args: ${args.mkString(" ")}")

    // Check if we already have CLI args (for local testing)
    val hasRealCliArgs = args.exists(arg => arg.startsWith("--") && !arg.startsWith("-D"))

    val effectiveArgs = if (hasRealCliArgs) {
      logger.info("Using provided CLI arguments directly")
      args
    } else {
      logger.info("No CLI args detected, reading from AWS Managed Flink application properties...")
      buildArgsFromApplicationProperties()
    }

    logger.info(s"Effective args for FlinkJob: ${effectiveArgs.mkString(" ")}")

    // Call the original FlinkJob main
    FlinkJob.main(effectiveArgs)
  }

  /** Read AWS Managed Flink application properties using reflection.
    *
    * AWS Managed Flink provides application properties via KinesisAnalyticsRuntime.getApplicationProperties()
    * which returns a Map<String, Properties>. We use reflection because this class is only available
    * in the AWS Managed Flink runtime, not in standard Flink JARs.
    *
    * The application properties are configured in the AWS console under "Application properties"
    * with property groups (e.g., "FlinkApplicationProperties").
    */
  private def getApplicationProperties(): JMap[String, String] = {
    try {
      // Load KinesisAnalyticsRuntime class (AWS Managed Flink specific)
      val runtimeClass = Class.forName("com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime")

      // Call KinesisAnalyticsRuntime.getApplicationProperties() via reflection
      // Returns Map<String, Properties>
      val getAppPropsMethod = runtimeClass.getMethod("getApplicationProperties")
      val allProperties = getAppPropsMethod.invoke(null).asInstanceOf[JMap[String, java.util.Properties]]

      logger.info(s"Found ${allProperties.size()} property groups: ${allProperties.keySet()}")

      // Flatten all property groups into a single map
      val flatMap = new java.util.HashMap[String, String]()

      allProperties.forEach { (groupId, props) =>
        logger.info(s"Processing property group: $groupId with ${props.size()} properties")
        props.forEach { (key, value) =>
          val strKey = key.toString
          val strValue = value.toString
          flatMap.put(strKey, strValue)
          logger.debug(s"  $groupId.$strKey = $strValue")
        }
      }

      flatMap
    } catch {
      case e: ClassNotFoundException =>
        throw new RuntimeException(
          "KinesisAnalyticsRuntime class not found. This wrapper is designed to run on AWS Managed Flink. " +
            "For local testing, pass CLI arguments directly.",
          e)
      case e: NoSuchMethodException =>
        throw new RuntimeException("getApplicationProperties() method not found on KinesisAnalyticsRuntime.", e)
      case e: Exception =>
        throw new RuntimeException(s"Failed to read application properties: ${e.getMessage}", e)
    }
  }

  /** Get a single property value from ParameterTool using reflection.
    */
  private def getProperty(propsMap: JMap[String, String], key: String): Option[String] = {
    Option(propsMap.get(key)).filter(_.nonEmpty)
  }

  /** Read AWS Managed Flink application properties and convert them to CLI-style arguments.
    */
  private def buildArgsFromApplicationProperties(): Array[String] = {
    val argsList = ListBuffer[String]()

    try {
      // Read from AWS Managed Flink application properties using reflection
      val propsMap = getApplicationProperties()
      val scalaPropsMap = propsMap.asScala

      logger.info(s"Found ${scalaPropsMap.size} application properties")
      scalaPropsMap.foreach { case (k, v) =>
        logger.debug(
          s"  Property: $k = ${if (k.toLowerCase.contains("secret") || k.toLowerCase.contains("key")) "***" else v}")
      }

      // Convert standard properties to CLI arguments
      PropertyToArgMapping.foreach { case (propName, argName) =>
        getProperty(propsMap, propName).foreach { value =>
          if (BooleanFlags.contains(propName)) {
            // For boolean flags, only add the flag if value is "true"
            if (value.toLowerCase == "true") {
              argsList += argName
            }
          } else {
            argsList += argName
            argsList += value
          }
          logger.info(s"Mapped property '$propName' -> '$argName' with value: $value")
        }
      }

      // Handle API properties (api.* -> -Z*)
      // These are passed to the Chronon API/KV Store configuration
      scalaPropsMap.foreach { case (key, value) =>
        if (key.startsWith("api.") && value.nonEmpty) {
          val propKey = key.stripPrefix("api.")
          argsList += s"-Z$propKey=$value"
          logger.info(s"Mapped API property 'api.$propKey' -> '-Z$propKey=$value'")
        }
      }

      // Handle ztasks (parallelism hint passed via -Ztasks)
      getProperty(propsMap, "ztasks").foreach { value =>
        argsList += s"-Ztasks=$value"
        logger.info(s"Mapped property 'ztasks' -> '-Ztasks=$value'")
      }

      // Validate we have required arguments
      val requiredProps = Seq("groupby-name", "online-class", "streaming-manifest-path")
      val missingProps = requiredProps.filterNot(prop => getProperty(propsMap, prop).isDefined)

      if (missingProps.nonEmpty) {
        val errorMsg = s"Missing required application properties: ${missingProps.mkString(", ")}. " +
          s"Please configure these in AWS Managed Flink console under 'FlinkApplicationProperties' property group."
        logger.error(errorMsg)
        throw new IllegalArgumentException(errorMsg)
      }

    } catch {
      case e: IllegalArgumentException => throw e
      case e: Exception =>
        val errorMsg = s"Failed to read AWS Managed Flink application properties: ${e.getMessage}"
        logger.error(errorMsg, e)
        throw new RuntimeException(errorMsg, e)
    }

    argsList.toArray
  }
}
