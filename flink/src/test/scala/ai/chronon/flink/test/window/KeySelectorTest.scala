package ai.chronon.flink.test.window

import ai.chronon.api.Builders
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.window.KeySelectorBuilder
import org.scalatest.flatspec.AnyFlatSpec

import java.util

class KeySelectorTest extends AnyFlatSpec {
  it should "chronon flink job correctly keys by a groupbys entity keys" in {
    // We expect something like this to come out of the SparkExprEval operator
    val sampleSparkExprEvalOutput: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "192.168.0.1", "user" -> "abc")
    val sparkEvalOutputWrapperEvent = ProjectedEvent(sampleSparkExprEvalOutput, 123L)

    val groupByWithOneEntityKey = Builders.GroupBy(keyColumns = Seq("number"))
    val keyFunctionOne = KeySelectorBuilder.build(groupByWithOneEntityKey)
    assert(
      keyFunctionOne.getKey(sparkEvalOutputWrapperEvent) == util.Arrays.asList(4242)
    )

    val groupByWithTwoEntityKey = Builders.GroupBy(keyColumns = Seq("number", "user"))
    val keyFunctionTwo = KeySelectorBuilder.build(groupByWithTwoEntityKey)
    assert(
      keyFunctionTwo.getKey(sparkEvalOutputWrapperEvent) == util.Arrays.asList(4242, "abc")
    )
  }

  it should "key selector function returns same hashes for lists with the same content" in {
    // This is more of a sanity check. It's not comprehensive.
    // SINGLE ENTITY KEY
    val map1: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "192.168.0.1", "user" -> "abc")
    val event1 = ProjectedEvent(map1, 123L)
    val map2: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "10.0.0.1", "user" -> "notabc")
    val event2 = ProjectedEvent(map2, 456L)
    val groupBySingleKey = Builders.GroupBy(keyColumns = Seq("number"))
    val keyFunctionOne = KeySelectorBuilder.build(groupBySingleKey)
    assert(
      keyFunctionOne.getKey(event1).hashCode() == keyFunctionOne.getKey(event2).hashCode()
    )

    // TWO ENTITY KEYS
    val map3: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "192.168.0.1", "user" -> "abc")
    val event3 = ProjectedEvent(map3, 123L)
    val map4: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "number" -> 4242, "user" -> "notabc")
    val event4 = ProjectedEvent(map4, 456L)
    val groupByTwoKeys = Builders.GroupBy(keyColumns = Seq("number", "ip"))
    val keyFunctionTwo = KeySelectorBuilder.build(groupByTwoKeys)
    assert(
      keyFunctionTwo.getKey(event3).hashCode() == keyFunctionTwo.getKey(event4).hashCode()
    )

    val map5: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "number" -> null)
    val event5 = ProjectedEvent(map5, 123L)
    val map6: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "number" -> null)
    val event6 = ProjectedEvent(map6, 456L)
    assert(keyFunctionTwo.getKey(event5).hashCode() == keyFunctionTwo.getKey(event6).hashCode())
  }
}
