package ai.chronon.api.test

import ai.chronon.api.ColumnExpression.getTimeExpression
import ai.chronon.api.{ColumnExpression, Query}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class TimeExpressionSpec extends AnyFlatSpec with Matchers {

  val TimeColumn = "ts"

  "getTimeExpression" should "return default time column when query is null" in {
    val result = getTimeExpression(null)
    result shouldEqual ColumnExpression(TimeColumn, None)
  }

  it should "use expression from selects when timeColumn is null and ts is in selects" in {
    val query = new Query()
    query.setSelects(Map("ts" -> "DATE_TRUNC(ts)").asJava)

    val result = getTimeExpression(query)
    result shouldEqual ColumnExpression(TimeColumn, Some("DATE_TRUNC(ts)"))
  }

  it should "use default ts when timeColumn is null and ts is not in selects" in {
    val query = new Query()
    query.setSelects(Map("other" -> "value").asJava)

    val result = getTimeExpression(query)
    result shouldEqual ColumnExpression(TimeColumn, None)
  }

  it should "use expression from selects when timeColumn matches select key" in {
    val query = new Query()
    query.setTimeColumn("timeMs")
    query.setSelects(Map("timeMs" -> "CAST(timeMs AS TIMESTAMP)").asJava)

    val result = getTimeExpression(query)
    result shouldEqual ColumnExpression(TimeColumn, Some("CAST(timeMs AS TIMESTAMP)"))
  }

  it should "use timeColumn as expression when timeColumn is set but not in selects" in {
    val query = new Query()
    query.setTimeColumn("timeMs")
    query.setSelects(Map("other" -> "value").asJava)

    val result = getTimeExpression(query)
    result shouldEqual ColumnExpression(TimeColumn, Some("timeMs"))
  }

  it should "use timeColumn directly when it's an expression" in {
    val query = new Query()
    query.setTimeColumn("DATE_TRUNC(day, timestamp)")
    query.setSelects(Map("other" -> "value").asJava)

    val result = getTimeExpression(query)
    result shouldEqual ColumnExpression(TimeColumn, Some("DATE_TRUNC(day, timestamp)"))
  }

  it should "handle null selects map" in {
    val query = new Query()
    query.setTimeColumn("timeMs")
    // not setting selects, so it will be null

    val result = getTimeExpression(query)
    result shouldEqual ColumnExpression(TimeColumn, Some("timeMs"))
  }

  // Helper function to simulate isIdentifier behavior

  // Safe get implementation

}
