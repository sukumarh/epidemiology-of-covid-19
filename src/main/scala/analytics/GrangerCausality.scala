package analytics

import org.apache.commons.math3.distribution.FDistribution
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

import scala.util.Random

object GrangerCausality {

  def testCausality(x: Array[Double], y: Array[Double], L: Int): (Double, Double) = {

    val random_gen = new Random()

    for (i <- x.indices){
      x(i) = x(i) + (0.00000001 * random_gen.nextInt(100))
      y(i) = y(i) + (0.00000001 * random_gen.nextInt(100))
    }

    val h0 = new OLSMultipleLinearRegression()
    val h1 = new OLSMultipleLinearRegression()

    val laggedY: Array[Array[Double]] = createLaggedSide(L, List(y))
    val laggedXY: Array[Array[Double]] = createLaggedSide(L, List(x, y))

    val n = laggedY.length

    h0.newSampleData(strip(L, y), laggedY)
    h1.newSampleData(strip(L, y), laggedXY)

    val rs0: Array[Double] = h0.estimateResiduals()
    val rs1: Array[Double] = h1.estimateResiduals()

    val RSS0 = sqrSum(rs0)
    val RSS1 = sqrSum(rs1)

    val ftest = ((RSS0 - RSS1) / L) / (RSS1 / (n - (2 * L)))

    val fDist = new FDistribution(L, n - 2 * L - 1)
    try {
      val pValue: Double = 1.0 - fDist.cumulativeProbability(ftest)
      (ftest, pValue)
    } catch {
      case e: Exception => println("e")
        (ftest, 1)
    }
  }

  private def createLaggedSide(L: Int, a: List[Array[Double]]): Array[Array[Double]] = {
    val n = a.head.length - L;

    val res = Array.fill[Array[Double]](n) {
      Array.fill[Double](L * a.length + 1) {
        0
      }
    }

    for (i <- a.indices) {
      val ai = a(i);
      for (l <- 0 until L) {
        for (j <- 0 until n) {
          res(j)(i * L + l) = ai(l + j);
        }
      }
    }

    for (i <- 0 until n) {
      res(i)(L * a.length) = 1;
    }

    res
  }

  private def sqrSum(a: Array[Double]): Double = {
    var res: Double = 0;
    for (v <- a) {
      res += v * v
    }
    res
  }

  private def strip(l: Int, a: Array[Double]): Array[Double] = {
    val res = Array.fill[Double](a.length - l) { 0 }
    Array.copy(a, l, res, 0, res.length)
    res
  }
}
