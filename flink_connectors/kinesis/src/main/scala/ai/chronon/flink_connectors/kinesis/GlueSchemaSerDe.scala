package ai.chronon.flink_connectors.kinesis

import ai.chronon.api.StructType
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroCodec, AvroConversions, AvroSerDe, Mutation, SerDe}
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  DefaultCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.{
  GetSchemaVersionRequest,
  GetSchemaVersionResponse,
  SchemaId,
  SchemaVersionNumber
}

/** SerDe that fetches schemas from AWS Glue Schema Registry and auto-detects the format (Avro or JSON).
  *
  * Configure via topic string:
  *   kinesis://stream-name/serde=glue_registry/registry_name=my-registry/schema_name=my-schema/[region=us-west-2]/[version_number=1]
  *
  * Parameters:
  *   - registry_name: Name of the Glue Schema Registry (required)
  *   - schema_name: Name of the schema in the registry (required)
  *   - region: AWS region (optional, uses AWS default region provider chain if not set)
  *   - version_number: Specific schema version (optional, defaults to latest)
  *   - aws_access_key_id / aws_secret_access_key: Explicit credentials (optional, both required if either is set)
  *
  * Local development:
  *   Set GLUE_LOCAL_SCHEMA_DIR env var to a directory containing schema files.
  *   Avro schemas: {schema_name}.avsc, JSON schemas: {schema_name}.json
  */
class GlueSchemaSerDe(topicInfo: TopicInfo) extends SerDe {
  import GlueSchemaSerDe._

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  (topicInfo.params.get(AccessKeyIdKey), topicInfo.params.get(SecretAccessKeyKey)) match {
    case (Some(_), Some(_)) | (None, None) => // valid combinations
    case _ =>
      throw new IllegalArgumentException(
        s"Both $AccessKeyIdKey and $SecretAccessKeyKey must be provided together, or neither for default credential chain"
      )
  }

  protected[flink_connectors] def buildGlueClient(): GlueClient = {
    val clientBuilder = GlueClient.builder()

    topicInfo.params.get(RegionKey).foreach(r => clientBuilder.region(Region.of(r)))

    (topicInfo.params.get(AccessKeyIdKey), topicInfo.params.get(SecretAccessKeyKey)) match {
      case (Some(accessKeyId), Some(secretAccessKey)) =>
        val credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey)
        clientBuilder.credentialsProvider(StaticCredentialsProvider.create(credentials))
      case _ =>
        clientBuilder.credentialsProvider(DefaultCredentialsProvider.builder().build())
    }

    clientBuilder.build()
  }

  private lazy val delegate: SerDe = retrieveSchema(topicInfo)

  private def retrieveSchema(topicInfo: TopicInfo): SerDe = {
    val schemaName =
      topicInfo.params.getOrElse(SchemaNameKey, throw new IllegalArgumentException(s"$SchemaNameKey not set"))

    val localSchemaDir = Option(System.getenv("GLUE_LOCAL_SCHEMA_DIR"))
    if (localSchemaDir.isDefined) {
      return loadLocalSchema(localSchemaDir.get, schemaName)
    }

    val registryName =
      topicInfo.params.getOrElse(RegistryNameKey, throw new IllegalArgumentException(s"$RegistryNameKey not set"))

    logger.info(s"Fetching schema from Glue: registry=$registryName, schema=$schemaName")

    val glueClient = buildGlueClient()

    val schemaId = SchemaId
      .builder()
      .registryName(registryName)
      .schemaName(schemaName)
      .build()

    val versionNumber = topicInfo.params.get(VersionNumberKey) match {
      case Some(v) => SchemaVersionNumber.builder().versionNumber(v.toLong).build()
      case None    => SchemaVersionNumber.builder().latestVersion(true).build()
    }

    val request = GetSchemaVersionRequest
      .builder()
      .schemaId(schemaId)
      .schemaVersionNumber(versionNumber)
      .build()

    val response: GetSchemaVersionResponse =
      try {
        glueClient.getSchemaVersion(request)
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"Failed retrieving schema from Glue Schema Registry - registry: $registryName, schema: $schemaName",
            e)
      } finally {
        glueClient.close()
      }

    val format = response.dataFormat().toString
    val schemaDefinition = response.schemaDefinition()

    logger.info(s"Retrieved schema (version ${response.versionNumber()}, format=$format) from Glue")

    format match {
      case "AVRO" =>
        val avroSchema: Schema = AvroCodec.of(schemaDefinition).schema
        new AvroSerDe(avroSchema)
      case "JSON" =>
        new JsonSchemaSerDe(schemaDefinition, schemaName)
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported schema format: $other. Supported formats are AVRO and JSON.")
    }
  }

  private def loadLocalSchema(dir: String, schemaName: String): SerDe = {
    val basePath = java.nio.file.Paths.get(dir)
    val avscFile = basePath.resolve(s"$schemaName.avsc")
    val jsonFile = basePath.resolve(s"$schemaName.json")

    if (java.nio.file.Files.exists(avscFile)) {
      logger.info(s"Loading local Avro schema from $avscFile")
      val schemaStr =
        new String(java.nio.file.Files.readAllBytes(avscFile), java.nio.charset.StandardCharsets.UTF_8)
      val avroSchema: Schema = AvroCodec.of(schemaStr).schema
      return new AvroSerDe(avroSchema)
    }

    if (java.nio.file.Files.exists(jsonFile)) {
      logger.info(s"Loading local JSON schema from $jsonFile")
      val schemaStr =
        new String(java.nio.file.Files.readAllBytes(jsonFile), java.nio.charset.StandardCharsets.UTF_8)
      return new JsonSchemaSerDe(schemaStr, schemaName)
    }

    throw new IllegalArgumentException(
      s"GLUE_LOCAL_SCHEMA_DIR is set but no schema file found: tried $avscFile and $jsonFile")
  }

  override def schema: StructType = delegate.schema

  override def fromBytes(bytes: Array[Byte]): Mutation = delegate.fromBytes(bytes)
}

object GlueSchemaSerDe {
  val RegionKey = "region"
  val AccessKeyIdKey = "aws_access_key_id"
  val SecretAccessKeyKey = "aws_secret_access_key"

  val RegistryNameKey = "registry_name"
  val SchemaNameKey = "schema_name"

  val VersionNumberKey = "version_number"
}
