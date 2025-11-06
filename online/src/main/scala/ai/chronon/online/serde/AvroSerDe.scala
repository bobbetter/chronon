package ai.chronon.online.serde

import ai.chronon.api.{Constants, StructType}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import com.linkedin.avro.fastserde.FastDeserializer
import java.io.{ByteArrayInputStream, InputStream}

class AvroSerDe(avroSchema: Schema) extends SerDe {

  lazy val chrononSchema: StructType = AvroConversions.toChrononSchema(avroSchema).asInstanceOf[StructType]

  @transient private lazy val avroToRowConverter = AvroConversions.genericRecordToChrononRowConverter(chrononSchema)

  lazy val schemaString: String = avroSchema.toString()

  def avroCodec: ThreadLocal[AvroCodec] = AvroCodec.ofThreaded(schemaString)

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    val avroRecord = avroCodec.get().decode(bytes)

    val row: Array[Any] = avroToRowConverter(avroRecord)

    val reversalIndex = schema.indexWhere(_.name == Constants.ReversalColumn)
    if (reversalIndex >= 0 && row(reversalIndex).asInstanceOf[Boolean]) {
      Mutation(schema, row, null)
    } else {
      Mutation(schema, null, row)
    }
  }

  override def schema: StructType = chrononSchema
}
