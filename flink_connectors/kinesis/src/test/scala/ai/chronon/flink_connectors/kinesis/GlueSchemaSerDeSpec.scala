package ai.chronon.flink_connectors.kinesis

import ai.chronon.online.TopicInfo
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.{DataFormat, GetSchemaVersionRequest, GetSchemaVersionResponse}

class MockGlueSchemaSerDe(topicInfo: TopicInfo, mockGlueClient: GlueClient) extends GlueSchemaSerDe(topicInfo) {
  override def buildGlueClient(): GlueClient = {
    mockGlueClient
  }
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

  it should "fail if the schema type is not AVRO" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.JSON)
      .schemaDefinition("{}")
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

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
}
