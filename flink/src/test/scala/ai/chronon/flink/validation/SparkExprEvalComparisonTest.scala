package ai.chronon.flink.validation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class SparkExprEvalComparisonTest extends AnyFlatSpec {

  it should "match empty result rows" in {
    val leftResult = Seq.empty
    val rightResult = Seq.empty
    val comparisonResult =
      SparkExprEvalComparisonFn.compareResultRows("recordId", leftResult, rightResult)
    comparisonResult.isMatch shouldBe true
  }

  it should "match when the result rows are the same" in {
    val leftResult = Seq(Map("a" -> 1, "b" -> 2L))
    val rightResult = Seq(Map("a" -> 1, "b" -> 2L))
    val comparisonResult =
      SparkExprEvalComparisonFn.compareResultRows("recordId", leftResult, rightResult)
    comparisonResult.isMatch shouldBe true
  }

  it should "match when results rows are the same with different order" in {
    val leftResult = Seq(Map("a" -> 1, "b" -> "2"), Map("a" -> 3, "b" -> "4"))
    val rightResult = Seq(Map("a" -> 3, "b" -> "4"), Map("a" -> 1, "b" -> "2"))
    val comparisonResult =
      SparkExprEvalComparisonFn.compareResultRows("recordId", leftResult, rightResult)
    comparisonResult.isMatch shouldBe true
  }

  it should "match when result rows contain complex types" in {
    val leftResult = Seq(
      Map("a" -> 1,
          "b" -> "2",
          "c" -> Array(50, 60),
          "d" -> Map("x" -> 100, "y" -> 200),
          "e" -> List(1, 2, 3),
          "f" -> Some(100)))
    val rightResult = Seq(
      Map("a" -> 1,
          "b" -> "2",
          "c" -> Array(50, 60),
          "d" -> Map("x" -> 100, "y" -> 200),
          "e" -> List(1, 2, 3),
          "f" -> Some(100)))
    val comparisonResult =
      SparkExprEvalComparisonFn.compareResultRows("recordId", leftResult, rightResult)
    comparisonResult.isMatch shouldBe true
  }

  it should "match when result rows contain nested complex types" in {
    val leftResult =
      Seq(Map("c" -> Array(List(5, 6)), "d" -> Map("x" -> Array(1, 0), "y" -> 200), "e" -> List(Some(1), Some(2))))
    val rightResult =
      Seq(Map("c" -> Array(List(5, 6)), "d" -> Map("x" -> Array(1, 0), "y" -> 200), "e" -> List(Some(1), Some(2))))
    val comparisonResult =
      SparkExprEvalComparisonFn.compareResultRows("recordId", leftResult, rightResult)
    comparisonResult.isMatch shouldBe true
  }

  it should "flag when the number of rows mismatch" in {
    val leftResult = Seq(Map("a" -> 1, "b" -> 2))
    val rightResult = Seq(Map("a" -> 1, "b" -> 2), Map("a" -> 3, "b" -> 4))
    val comparisonResult =
      SparkExprEvalComparisonFn.compareResultRows("recordId", leftResult, rightResult)
    comparisonResult.isMatch shouldBe false
    comparisonResult.differences.contains("result_count") shouldBe true
  }

  it should "flag when the row values mismatch" in {
    val leftResult = Seq(Map("a" -> 1, "b" -> 2))
    val rightResult = Seq(Map("a" -> 1, "b" -> 30))
    val comparisonResult =
      SparkExprEvalComparisonFn.compareResultRows("recordId", leftResult, rightResult)
    comparisonResult.isMatch shouldBe false
    comparisonResult.differences.contains("result_row_value_0_b") shouldBe true
  }

  it should "flag when the row keys mismatch" in {
    val leftResult = Seq(Map("a" -> 1, "b" -> 2))
    val rightResult = Seq(Map("a" -> 1, "c" -> 2))
    val comparisonResult =
      SparkExprEvalComparisonFn.compareResultRows("recordId", leftResult, rightResult)
    comparisonResult.isMatch shouldBe false
    comparisonResult.differences.contains("result_row_value_0_b") shouldBe true
  }

  it should "flag when a row has more fields than the other" in {
    val leftResult = Seq(Map("a" -> 1, "b" -> 2))
    val rightResult = Seq(Map("a" -> 1))
    val comparisonResult =
      SparkExprEvalComparisonFn.compareResultRows("recordId", leftResult, rightResult)
    comparisonResult.isMatch shouldBe false
    comparisonResult.differences.contains("result_row_size_0") shouldBe true
  }

  it should "flag when result rows contain complex types that mismatch" in {
    val leftResult = Seq(
      Map("a" -> 1,
          "b" -> "2",
          "c" -> Array(50, 60),
          "d" -> Map("x" -> 100, "y" -> 200),
          "e" -> List(1, 2, 3),
          "f" -> Some(100)))
    val rightResult = Seq(
      Map("a" -> 1,
          "b" -> "2",
          "c" -> Array(55, 65),
          "d" -> Map("x" -> 110, "y" -> 210),
          "e" -> List(1, 2, 5),
          "f" -> Some(1000)))
    val comparisonResult =
      SparkExprEvalComparisonFn.compareResultRows("recordId", leftResult, rightResult)
    comparisonResult.isMatch shouldBe false
    comparisonResult.differences.contains("result_row_value_0_c") shouldBe true
    comparisonResult.differences.contains("result_row_value_0_d") shouldBe true
    comparisonResult.differences.contains("result_row_value_0_e") shouldBe true
    comparisonResult.differences.contains("result_row_value_0_f") shouldBe true
  }

  it should "flag when result rows contain nested complex types that mismatch" in {
    val leftResult =
      Seq(Map("c" -> Array(List(5, 6)), "d" -> Map("x" -> Array(1, 0), "y" -> 200), "e" -> List(Some(1), Some(2))))
    val rightResult =
      Seq(Map("c" -> Array(List(6, 5)), "d" -> Map("x" -> Array(10, 20), "y" -> 200), "e" -> List(Some(7), Some(2))))
    val comparisonResult =
      SparkExprEvalComparisonFn.compareResultRows("recordId", leftResult, rightResult)
    comparisonResult.isMatch shouldBe false
    comparisonResult.differences.contains("result_row_value_0_c") shouldBe true
    comparisonResult.differences.contains("result_row_value_0_d") shouldBe true
    comparisonResult.differences.contains("result_row_value_0_e") shouldBe true
  }

}
