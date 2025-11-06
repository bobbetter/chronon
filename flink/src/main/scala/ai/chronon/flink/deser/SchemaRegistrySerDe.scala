package ai.chronon.flink.deser

import ai.chronon.api.StructType
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroConversions, AvroSerDe, Mutation, SerDe}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro.Schema

/** Schema Provider / SerDe implementation that uses the Confluent Schema Registry to fetch schemas for topics.
  * Can be configured as: topic = "kafka://topic-name/registry_host=host/[registry_port=port]/[registry_scheme=http]/[subject=subject]"
  * Port, scheme and subject are optional. If port is missing, we assume the host is pointing to a LB address / such that
  * forwards to the right host + port. Scheme defaults to http. Subject defaults to the topic name + "-value" (based on schema
  * registry conventions).
  */
class SchemaRegistrySerDe(topicInfo: TopicInfo) extends SerDe {
  import SchemaRegistrySerDe._

  private val schemaRegistryHost: String =
    topicInfo.params.getOrElse(RegistryHostKey, throw new IllegalArgumentException(s"$RegistryHostKey not set"))

  // port is optional as many folks configure just the host as it's behind an LB
  private val schemaRegistryPortString: Option[String] = topicInfo.params.get(RegistryPortKey)

  // default to http if not set
  private val schemaRegistrySchemeString: String = topicInfo.params.getOrElse(RegistrySchemeKey, "http")

  private val CacheCapacity: Int = 10

  private val schemaRegistryWireFormat: Boolean =
    topicInfo.params.getOrElse(SchemaRegistryWireFormat, "true").toBoolean

  protected[flink] def buildSchemaRegistryClient(schemeString: String,
                                                 registryHost: String,
                                                 maybePortString: Option[String]): SchemaRegistryClient = {
    maybePortString match {
      case Some(portString) =>
        val registryUrl = s"$schemeString://$registryHost:$portString"
        new CachedSchemaRegistryClient(registryUrl, CacheCapacity)
      case None =>
        val registryUrl = s"$schemeString://$registryHost"
        new CachedSchemaRegistryClient(registryUrl, CacheCapacity)
    }
  }

  lazy val (avroSchema, chrononSchema) = retrieveTopicSchema(topicInfo)

  private def retrieveTopicSchema(topicInfo: TopicInfo): (Schema, StructType) = {
    val schemaRegistryClient =
      buildSchemaRegistryClient(schemaRegistrySchemeString, schemaRegistryHost, schemaRegistryPortString)
    val subject = topicInfo.params.getOrElse(RegistrySubjectKey, s"${topicInfo.name}-value")
    val parsedSchema =
      try {
        val metadata = schemaRegistryClient.getLatestSchemaMetadata(subject)
        schemaRegistryClient.getSchemaById(metadata.getId)
      } catch {
        case e: RestClientException =>
          throw new IllegalArgumentException(
            s"Failed to retrieve schema details from the registry. Status: ${e.getStatus}; Error code: ${e.getErrorCode}",
            e)
        case e: Exception =>
          throw new IllegalArgumentException("Error connecting to and requesting schema details from the registry", e)
      }
    require(parsedSchema.schemaType() == AvroSchema.TYPE,
            s"Unsupported schema type: ${parsedSchema.schemaType()}. Only Avro is supported.")
    val avroSchema: Schema = parsedSchema.asInstanceOf[AvroSchema].rawSchema()
    val chrononSchema: StructType = AvroConversions.toChrononSchema(avroSchema).asInstanceOf[StructType]
    (avroSchema, chrononSchema)
  }

  override def schema: StructType = chrononSchema

  lazy val avroSerDe = new AvroSerDe(avroSchema)

  override def fromBytes(message: Array[Byte]): Mutation = {
    val messageBytes =
      if (schemaRegistryWireFormat) {
        // schema id is set, we skip the first byte and read the schema id based on the wire format:
        // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#messages-wire-format
        // unfortunately we need to drop the first 5 bytes (and thus copy the rest of the byte array) as the AvroDataToCatalyst
        // interface takes a byte array and the methods to do the Row conversion etc are all private so we can't reach in
        message.drop(5)
      } else {
        message
      }
    avroSerDe.fromBytes(messageBytes)
  }
}

object SchemaRegistrySerDe {
  val RegistryHostKey = "registry_host"
  val RegistryPortKey = "registry_port"
  val RegistrySchemeKey = "registry_scheme"
  val RegistrySubjectKey = "subject"
  val SchemaRegistryWireFormat = "schema_registry_wire_format"
}
