package ai.chronon.flink_connectors.kinesis

import ai.chronon.api.StructType
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroCodec, AvroConversions, AvroSerDe, Mutation, SerDe}
import org.apache.avro.Schema
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

/** SerDe implementation that uses AWS Glue Schema Registry to fetch Avro schemas.
  * Can be configured as: topic = "kinesis://stream-name/registry_name=my-registry/schema_name=my-schema/[region=us-west-2]/[version_number=1]"
  * Region and version_number are optional. If region is missing, the AWS default region provider chain is used.
  * If version_number is missing, the latest schema version is used.
  */
class GlueSchemaSerDe(topicInfo: TopicInfo) extends SerDe {
  import GlueSchemaSerDe._

  // Validate credentials configuration early
  (topicInfo.params.get(AccessKeyIdKey), topicInfo.params.get(SecretAccessKeyKey)) match {
    case (Some(_), Some(_)) | (None, None) => // valid combinations
    case _ =>
      throw new IllegalArgumentException(
        s"Both $AccessKeyIdKey and $SecretAccessKeyKey must be provided together, or neither for default credential chain"
      )
  }

  protected[flink_connectors] def buildGlueClient(): GlueClient = {
    val clientBuilder = GlueClient.builder()

    // Region is optional - if not provided, SDK uses default region provider chain
    // (AWS_REGION env var, aws.region system property, ~/.aws/config, EC2 metadata)
    topicInfo.params.get(RegionKey).foreach(r => clientBuilder.region(Region.of(r)))

    // credential provider selection:
    // - BASIC: When explicit credentials provided via topic params
    // - DEFAULT: When no credentials provided (uses AWS credential chain)
    (topicInfo.params.get(AccessKeyIdKey), topicInfo.params.get(SecretAccessKeyKey)) match {
      case (Some(accessKeyId), Some(secretAccessKey)) =>
        val credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey)
        clientBuilder.credentialsProvider(StaticCredentialsProvider.create(credentials))
      case (None, None) =>
        clientBuilder.credentialsProvider(DefaultCredentialsProvider.builder().build())
      case _ =>
        // This case should never be reached due to validation above
        throw new IllegalArgumentException(
          s"Both $AccessKeyIdKey and $SecretAccessKeyKey must be provided together, or neither for default credential chain"
        )
    }

    clientBuilder.build()
  }

  lazy val (avroSchemaStr, chrononSchema) = retrieveSchema(topicInfo)

  private def retrieveSchema(topicInfo: TopicInfo): (String, StructType) = {
    val glueClient = buildGlueClient()

    val registryName =
      topicInfo.params.getOrElse(RegistryNameKey, throw new IllegalArgumentException(s"$RegistryNameKey not set"))
    val schemaName =
      topicInfo.params.getOrElse(SchemaNameKey, throw new IllegalArgumentException(s"$SchemaNameKey not set"))

    val schemaId = SchemaId
      .builder()
      .registryName(registryName)
      .schemaName(schemaName)
      .build()

    // Use latest version by default, or specific version if provided
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

    require(response.dataFormat().toString == "AVRO",
            s"Unsupported schema type: ${response.dataFormat()}. Only Avro is supported.")

    val avroSchemaStr = response.schemaDefinition()
    val avroSchema: Schema = AvroCodec.of(avroSchemaStr).schema
    val chrononSchema: StructType = AvroConversions.toChrononSchema(avroSchema).asInstanceOf[StructType]
    (avroSchemaStr, chrononSchema)
  }

  override def schema: StructType = chrononSchema

  lazy val avroSerDe = {
    val avroSchema = AvroCodec.of(avroSchemaStr).schema
    new AvroSerDe(avroSchema)
  }

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    avroSerDe.fromBytes(bytes)
  }
}

object GlueSchemaSerDe {
  val RegionKey = "region"
  val AccessKeyIdKey = "aws_access_key_id"
  val SecretAccessKeyKey = "aws_secret_access_key"

  // Schema identification (required)
  val RegistryNameKey = "registry_name"
  val SchemaNameKey = "schema_name"

  // Optional: defaults to latest version if not specified
  val VersionNumberKey = "version_number"
}
