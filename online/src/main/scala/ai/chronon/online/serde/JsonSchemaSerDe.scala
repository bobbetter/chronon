package ai.chronon.online.serde

import ai.chronon.api.{Constants, StructType}
import com.fasterxml.jackson.databind.ObjectMapper

import java.util

/** SerDe for JSON-encoded messages with schemas defined in JSON Schema format (https://json-schema.org/).
  *
  * Schema parsing and value conversion are handled by [[JsonConversions]].
  */
class JsonSchemaSerDe(jsonSchemaDefinition: String, schemaName: String) extends SerDe {

  @transient private lazy val objectMapper: ThreadLocal[ObjectMapper] =
    ThreadLocal.withInitial(() => new ObjectMapper())

  lazy val chrononSchema: StructType = {
    val schemaDef = objectMapper.get().readValue(jsonSchemaDefinition, classOf[util.LinkedHashMap[String, AnyRef]])
    JsonConversions.toChrononSchema(schemaDef, schemaName)
  }

  override def schema: StructType = chrononSchema

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    val parsed = objectMapper.get().readValue(bytes, classOf[util.LinkedHashMap[String, AnyRef]])
    val row = JsonConversions.toChrononRow(parsed, chrononSchema)
    val reversalIndex = chrononSchema.indexWhere(_.name == Constants.ReversalColumn)
    if (reversalIndex >= 0 && row(reversalIndex).asInstanceOf[Boolean]) {
      Mutation(chrononSchema, row, null)
    } else {
      Mutation(chrononSchema, null, row)
    }
  }
}
