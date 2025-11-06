package ai.chronon.online.test.stats

import ai.chronon.online.stats.AssignIntervals
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AssignIntervalsTest extends AnyFlatSpec with Matchers {

  "assignment" should "assign weights into intervals between breaks" in {
    val percentiles = Array(1, 4, 6, 6, 6, 8, 9)
    val breaks = Array(0, 1, 2, 3, 5, 6, 7, 8, 9, 10)

    // val interval = 0.25
    val expected = Array(0.0, 1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0 + 1.0 / 2.0, 1.0 / 2.0, 2.5, 0.5, 1, 0)

    val result = AssignIntervals.on(ptiles = percentiles.map(_.toDouble), breaks = breaks.map(_.toDouble))

    expected.zip(result).foreach { case (e, r) =>
      println(s"exp: $e res: $r")
      r shouldEqual e
    }
  }
}
