package ai.chronon.flink_connectors.kinesis

import ai.chronon.online.TopicInfo
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.{DataFormat, GetSchemaVersionRequest, GetSchemaVersionResponse}

import java.nio.file.Files
import java.nio.charset.StandardCharsets

class MockGlueSchemaSerDe(topicInfo: TopicInfo, mockGlueClient: GlueClient) extends GlueSchemaSerDe(topicInfo) {
  override def buildGlueClient(): GlueClient = {
    mockGlueClient
  }
}

class MockGlueSchemaSerDeWithLocalDir(topicInfo: TopicInfo, localDir: String) extends GlueSchemaSerDe(topicInfo) {
  override def localSchemaDirOverride: Option[String] = Some(localDir)
  override def buildGlueClient(): GlueClient = throw new UnsupportedOperationException("should not call Glue")
}

class GlueSchemaSerDeSpec extends AnyFlatSpec {
  it should "fail if registry_name is not provided" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))
    val mockGlueClient = mock[GlueClient]

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    assertThrows[IllegalArgumentException] {
      glueSchemaSerDe.schema
    }
  }

  it should "fail if schema_name is not provided" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry"
      ))
    val mockGlueClient = mock[GlueClient]

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    assertThrows[IllegalArgumentException] {
      glueSchemaSerDe.schema
    }
  }

  it should "succeed without region (uses default region provider chain)" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.AVRO)
      .schemaDefinition(avroSchemaStr)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    val deSerSchema = glueSchemaSerDe.schema
    assert(deSerSchema != null)
  }

  it should "fail if credentials are partially provided" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema",
        GlueSchemaSerDe.AccessKeyIdKey -> "access-key-only"
      ))
    val mockGlueClient = mock[GlueClient]

    assertThrows[IllegalArgumentException] {
      new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    }
  }

  it should "fail if the schema is not found" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "non-existent-registry",
        GlueSchemaSerDe.SchemaNameKey -> "non-existent-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]()))
      .thenThrow(new RuntimeException("Schema not found"))

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    assertThrows[IllegalArgumentException] {
      glueSchemaSerDe.schema
    }
  }

  it should "succeed if the schema is found and is of type AVRO" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.AVRO)
      .schemaDefinition(avroSchemaStr)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    val deSerSchema = glueSchemaSerDe.schema
    assert(deSerSchema != null)
  }

  it should "auto-detect JSON format and parse JSON Schema" in {
    val jsonSchema =
      """{
        |  "title": "login_event",
        |  "type": "object",
        |  "properties": {
        |    "user_id": { "type": "string" },
        |    "ts": { "type": "integer" },
        |    "success": { "type": "boolean" }
        |  }
        |}""".stripMargin

    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "login_event"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.JSON)
      .schemaDefinition(jsonSchema)
      .versionNumber(1L)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 3)
    assert(schema.fields.exists(_.name == "user_id"))
    assert(schema.fields.exists(_.name == "ts"))
    assert(schema.fields.exists(_.name == "success"))
  }

  it should "deserialize JSON messages when Glue schema is JSON format" in {
    val jsonSchema =
      """{
        |  "title": "login_event",
        |  "type": "object",
        |  "properties": {
        |    "user_id": { "type": "string" },
        |    "ts": { "type": "integer" },
        |    "score": { "type": "number" }
        |  }
        |}""".stripMargin

    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "login_event"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.JSON)
      .schemaDefinition(jsonSchema)
      .versionNumber(1L)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)

    val message = """{"user_id": "abc123", "ts": 1700000000000, "score": 0.95}"""
    val mutation = serDe.fromBytes(message.getBytes(java.nio.charset.StandardCharsets.UTF_8))

    assert(mutation.after != null)
    assert(mutation.after(0) == "abc123")
    assert(mutation.after(1) == 1700000000000L)
    assert(mutation.after(2) == 0.95)
  }

  it should "fall back to LocalSchemaSerDe when LOCAL_SCHEMA_DIR override is set" in {
    val tmpDir = Files.createTempDirectory("glue-local-fallback-test")
    val avroSchemaStr =
      """{ "type": "record", "name": "test_schema", "fields": [ { "type": "string", "name": "id" } ] }"""
    Files.write(tmpDir.resolve("test-schema.avsc"), avroSchemaStr.getBytes(StandardCharsets.UTF_8))

    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))

    val serDe = new MockGlueSchemaSerDeWithLocalDir(topicInfo, tmpDir.toString)
    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.exists(_.name == "id"))
  }

  it should "fail for unsupported schema formats" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.PROTOBUF)
      .schemaDefinition("{}")
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    assertThrows[IllegalArgumentException] {
      glueSchemaSerDe.schema
    }
  }
}
