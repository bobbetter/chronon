package ai.chronon.online.serde

import ai.chronon.api.StructType
import ai.chronon.online.TopicInfo
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** SerDe that loads schema files from the local filesystem, auto-detecting format by extension (.avsc or .json).
  *
  * Configure via topic string:
  *   kafka://topic-name/serde=localfs/schema_name=my-schema/[schema_dir=/path/to/schemas]
  *
  * Parameters:
  *   - schema_name: Base name of the schema file, without extension (required)
  *   - schema_dir: Directory containing schema files (optional; falls back to LOCAL_SCHEMA_DIR env var)
  */
class LocalSchemaSerDe(topicInfo: TopicInfo) extends SerDe {
  import LocalSchemaSerDe._

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  private lazy val delegate: SerDe = {
    val schemaName =
      topicInfo.params.getOrElse(SchemaNameKey, throw new IllegalArgumentException(s"$SchemaNameKey not set"))

    val schemaDir = topicInfo.params
      .get(SchemaDirKey)
      .orElse(Option(System.getenv(SchemaDirEnvVar)))
      .getOrElse(throw new IllegalArgumentException(
        s"Schema directory not set: provide '$SchemaDirKey' in topic params or set the $SchemaDirEnvVar env var"))

    loadSchema(schemaDir, schemaName)
  }

  private def loadSchema(dir: String, schemaName: String): SerDe = {
    val basePath = Paths.get(dir)
    val avscFile = basePath.resolve(s"$schemaName.avsc")
    val jsonFile = basePath.resolve(s"$schemaName.json")

    if (Files.exists(avscFile)) {
      logger.info(s"Loading local Avro schema from $avscFile")
      val schemaStr = new String(Files.readAllBytes(avscFile), StandardCharsets.UTF_8)
      val avroSchema = AvroCodec.of(schemaStr).schema
      return new AvroSerDe(avroSchema)
    }

    if (Files.exists(jsonFile)) {
      logger.info(s"Loading local JSON schema from $jsonFile")
      val schemaStr = new String(Files.readAllBytes(jsonFile), StandardCharsets.UTF_8)
      return new JsonSchemaSerDe(schemaStr, schemaName)
    }

    throw new IllegalArgumentException(
      s"No schema file found for '$schemaName' in $dir: tried $avscFile and $jsonFile")
  }

  override def schema: StructType = delegate.schema

  override def fromBytes(bytes: Array[Byte]): Mutation = delegate.fromBytes(bytes)
}

object LocalSchemaSerDe {
  val SchemaNameKey = "schema_name"
  val SchemaDirKey = "schema_dir"
  val SchemaDirEnvVar = "LOCAL_SCHEMA_DIR"
}
