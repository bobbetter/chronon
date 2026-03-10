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

  private def serDeWithDir(topicInfo: TopicInfo, dir: String): LocalSchemaSerDe =
    new LocalSchemaSerDe(topicInfo, dir)

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

    val serDe = serDeWithDir(makeTopicInfo(Map("schema_name" -> "event")), dir.toString)

    serDe.schema.name shouldBe "event"
    serDe.schema.fields.find(_.name == "id").get.fieldType shouldBe StringType
    serDe.schema.fields.find(_.name == "ts").get.fieldType shouldBe LongType

    val mutation = serDe.fromBytes("""{"id":"abc","ts":1234567890}""".getBytes(StandardCharsets.UTF_8))
    mutation.before shouldBe null
    val schema = serDe.schema
    mutation.after(schema.indexWhere(_.name == "id")) shouldBe "abc"
    mutation.after(schema.indexWhere(_.name == "ts")) shouldBe 1234567890L
  }

  it should "throw when schema_name is missing" in withTempDir { dir =>
    an[IllegalArgumentException] should be thrownBy serDeWithDir(makeTopicInfo(Map.empty), dir.toString).schema
  }

  it should "throw when no matching schema file exists in the directory" in withTempDir { dir =>
    an[IllegalArgumentException] should be thrownBy
      serDeWithDir(makeTopicInfo(Map("schema_name" -> "missing")), dir.toString).schema
  }
}
