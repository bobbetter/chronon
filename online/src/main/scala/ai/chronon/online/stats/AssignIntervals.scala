package ai.chronon.online.stats

// given two sorted arrays -
//   one containing percentiles, with equally spaced "intervals" - eg., (p0, p5, p10,...p100) interval = 5%
//   another containing breaks
//   produce a resulting array of size `breaks.length - 1`
//   where each index of resulting array contains the number of intervals in `breaks(i) - breaks(i+1)`
//   if there is partial overlap, or if the break point is in between the interval, the interval needs to
//   be assigned to both the breaks on either side of the breakpoint in a "fraction"-al manner

// note: the interval convention is left inclusive, right exclusive
//       please see the test case to understand the algorithm better
// at a high level we are converting equally percentile points into histograms / distributions

object AssignIntervals {
  def on(ptiles: Array[Double], breaks: Array[Double]): Array[Double] = {
    var i = 0
    var prev = breaks.head
    var bIndex = 1
    val bLen = breaks.length
    val pLen = ptiles.length
    val arr = Array.fill(bLen - 1)(0.0)

    // discard before b(0)
    while (i < pLen && ptiles(i) < breaks(0)) {
      i += 1
    }

    while (bIndex < bLen) {
      var result = 0.0
      val b = breaks(bIndex)

      while (i < pLen && ptiles(i) < b) {

        // fraction of the equally spaced interval between p(i-1) to p(i) to assign to this break
        val fraction =
          if (i == 0) 0.0
          else if (ptiles(i) == ptiles(i - 1)) 1.0
          else (ptiles(i) - prev) / (ptiles(i) - ptiles(i - 1))

        result += fraction
        prev = ptiles(i)
        i += 1
      }

      // residual fraction to assign
      val fraction =
        if (i <= 0 || i >= pLen) 0.0
        else (b - prev) / (ptiles(i) - ptiles(i - 1))

      result += fraction
      prev = b
      arr.update(bIndex - 1, result)
      bIndex += 1
    }
    arr
  }
}
