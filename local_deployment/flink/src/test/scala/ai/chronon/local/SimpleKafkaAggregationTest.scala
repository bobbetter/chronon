package ai.chronon.local

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SimpleKafkaAggregationTest extends AnyFlatSpec with Matchers {

  behavior of "parseJsonLine"

  it should "parse valid JSON with user only" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"alice"}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("alice", 1L))
  }

  it should "parse valid JSON with user and val" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"bob", "val": 5}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("bob", 5L))
  }

  it should "parse valid JSON with user and value" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"charlie", "value": 10}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("charlie", 10L))
  }

  it should "parse JSON with extra whitespace" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""  { "user" : "dave" , "val" : 3 }  """)
    result shouldBe Some(SimpleKafkaAggregation.Event("dave", 3L))
  }

  it should "handle negative values" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"eve", "val": -7}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("eve", -7L))
  }

  it should "default to value 1 when val/value is not provided" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"frank"}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("frank", 1L))
  }

  it should "ignore extra unknown fields" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"grace", "val": 2, "extra": "ignored"}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("grace", 2L))
  }

  it should "return None when user field is missing" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"val": 5}""")
    result shouldBe None
  }

  it should "return None for empty string" in {
    val result = SimpleKafkaAggregation.parseJsonLine("")
    result shouldBe None
  }

  it should "return None when JSON doesn't start with {" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""user":"alice"}""")
    result shouldBe None
  }

  it should "return None when JSON doesn't end with }" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"alice"""")
    result shouldBe None
  }

  it should "handle JSON with key but no colon (parser quirk)" in {
    // The parser treats this as a key without a value, which doesn't match a key-value pattern
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user"}""")
    result shouldBe None
  }

  it should "return None for non-JSON string" in {
    val result = SimpleKafkaAggregation.parseJsonLine("not json at all")
    result shouldBe None
  }

  it should "handle value with trailing non-digit characters" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"henry", "val": 123abc}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("henry", 123L))
  }

  it should "handle zero value" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"iris", "val": 0}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("iris", 0L))
  }

  it should "handle very large values" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"jack", "val": 9223372036854775807}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("jack", 9223372036854775807L))
  }

  it should "handle user names with special characters (within quotes)" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"user@example.com", "val": 4}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("user@example.com", 4L))
  }

  it should "handle reversed field order" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"val": 7, "user":"kate"}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("kate", 7L))
  }

  it should "handle value field when both val and value are present (last one wins)" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"leo", "val": 5, "value": 8}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("leo", 8L))
  }

  it should "handle empty user string" in {
    val result = SimpleKafkaAggregation.parseJsonLine("""{"user":"", "val": 3}""")
    result shouldBe Some(SimpleKafkaAggregation.Event("", 3L))
  }

  it should "handle only braces" in {
    val result = SimpleKafkaAggregation.parseJsonLine("{}")
    result shouldBe None
  }
}

