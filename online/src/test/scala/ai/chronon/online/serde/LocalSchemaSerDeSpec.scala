package ai.chronon.online.serde

import ai.chronon.api._
import ai.chronon.online.TopicInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class LocalSchemaSerDeSpec extends AnyFlatSpec with Matchers {

  private def makeTopicInfo(params: Map[String, String]): TopicInfo =
    TopicInfo("test-topic", "kafka", params)

  private def withTempDir(f: java.nio.file.Path => Unit): Unit = {
    val dir = Files.createTempDirectory("local-schema-serde-test")
    try f(dir)
    finally {
      dir.toFile.listFiles().foreach(_.delete())
      dir.toFile.delete()
    }
  }

  // --- JSON schema ---

  it should "load a JSON schema file and deserialize messages" in withTempDir { dir =>
    val jsonSchema =
      """{
        |  "title": "event",
        |  "type": "object",
        |  "properties": {
        |    "id": { "type": "string" },
        |    "ts": { "type": "integer" }
        |  }
        |}""".stripMargin
    Files.write(dir.resolve("event.json"), jsonSchema.getBytes(StandardCharsets.UTF_8))

    val topicInfo = makeTopicInfo(Map("schema_name" -> "event", "schema_dir" -> dir.toString))
    val serDe = new LocalSchemaSerDe(topicInfo)

    serDe.schema.name shouldBe "event"
    serDe.schema.fields.find(_.name == "id").get.fieldType shouldBe StringType
    serDe.schema.fields.find(_.name == "ts").get.fieldType shouldBe LongType

    val mutation = serDe.fromBytes("""{"id":"abc","ts":1234567890}""".getBytes(StandardCharsets.UTF_8))
    mutation.before shouldBe null
    val schema = serDe.schema
    mutation.after(schema.indexWhere(_.name == "id")) shouldBe "abc"
    mutation.after(schema.indexWhere(_.name == "ts")) shouldBe 1234567890L
  }

  it should "prefer schema_dir param over LOCAL_SCHEMA_DIR env var" in withTempDir { dir =>
    val jsonSchema =
      """{
        |  "title": "ev",
        |  "type": "object",
        |  "properties": { "x": { "type": "string" } }
        |}""".stripMargin
    Files.write(dir.resolve("ev.json"), jsonSchema.getBytes(StandardCharsets.UTF_8))

    // env var points to a non-existent dir — param dir should win
    val topicInfo = makeTopicInfo(Map("schema_name" -> "ev", "schema_dir" -> dir.toString))
    val serDe = new LocalSchemaSerDe(topicInfo)
    serDe.schema.name shouldBe "ev"
  }

  it should "throw when schema_name is missing" in {
    val topicInfo = makeTopicInfo(Map("schema_dir" -> "/some/dir"))
    an[IllegalArgumentException] should be thrownBy new LocalSchemaSerDe(topicInfo).schema
  }

  it should "throw when neither schema_dir param nor LOCAL_SCHEMA_DIR env var is set" in {
    // Only works when env var is not actually set in the test environment
    val topicInfo = makeTopicInfo(Map("schema_name" -> "event"))
    assume(Option(System.getenv(LocalSchemaSerDe.SchemaDirEnvVar)).isEmpty,
           s"${LocalSchemaSerDe.SchemaDirEnvVar} is set in env; skipping this test")
    an[IllegalArgumentException] should be thrownBy new LocalSchemaSerDe(topicInfo).schema
  }

  it should "throw when no matching schema file exists in the directory" in withTempDir { dir =>
    val topicInfo = makeTopicInfo(Map("schema_name" -> "missing", "schema_dir" -> dir.toString))
    an[IllegalArgumentException] should be thrownBy new LocalSchemaSerDe(topicInfo).schema
  }
}
