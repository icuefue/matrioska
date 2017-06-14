package com.bbva.ebdm.linx.ingest.utils

import com.bbva.ebdm.linx.ingest.utils.FaultToleranceTests.FaultToleranceTest
import org.apache.spark.rdd.RDD
import com.bbva.ebdm.linx.core.exceptions.FatalException
import com.bbva.ebdm.linx.ingest.utils.FaultToleranceTests.RisksVoidFileTest
import com.bbva.ebdm.linx.ingest.utils.FaultToleranceTests.TestWithParameters
import com.bbva.ebdm.linx.ingest.utils.FaultToleranceTests.RisksVoidMatrixFileTest
import com.bbva.ebdm.linx.ingest.utils.FaultToleranceTests.RisksVoidDeltaFxFileTest

object FaultToleranceTests {
  sealed trait FaultToleranceTest
  case object RisksVoidFileTest extends FaultToleranceTest
  case object RisksVoidMatrixFileTest extends FaultToleranceTest
  case object RisksVoidDeltaFxFileTest extends FaultToleranceTest
  case class TestWithParameters(parameter1: String, parameter2: String) extends FaultToleranceTest

  def getFaultToleranceTest(codToleranceTest: Int, parameters: String): Option[FaultToleranceTest] = {
    codToleranceTest match {
      case 0 => None
      case 1 => Some(RisksVoidFileTest)
      case 2 => Some(RisksVoidMatrixFileTest)
      case 3 =>
        val params = parameters.split(",")
        Some(TestWithParameters(params(0), params(1)))
      case 4 => Some(RisksVoidDeltaFxFileTest)

      case default => throw new FatalException("No existe el test de tipo:" + default)
    }
  }
  val faultToleranceTests = Seq(RisksVoidFileTest, TestWithParameters)
}

object FaultToleranceUtils {

  def check(tolerances: Seq[FaultToleranceTest], rdd: RDD[Tuple2[Seq[Any], Int]], numberOks: Long, total: Long): Boolean = {
    var result = false
    for (tolerance <- tolerances) {
      tolerance match {
        case RisksVoidFileTest => result = risksVoidFileTest(numberOks: Long, total: Long)
        case RisksVoidMatrixFileTest => result = risksVoidMatrixFileTest(numberOks: Long, total: Long)
        case TestWithParameters(_, _) => result = test
        case RisksVoidDeltaFxFileTest => result = risksVoidDeltaFxFileTest(numberOks: Long, total: Long)
        case default =>
      }
    }
    result
  }

  def risksVoidFileTest(numberOks: Long, total: Long): Boolean = {

    var result = false
    if (numberOks == 0 && total == 1) {
      result = true
    }
    println("\n\n\nrisksVoidFileTest: " + numberOks + " y total: " + total)
    result
  }

  def risksVoidMatrixFileTest(numberOks: Long, total: Long): Boolean = {

    var result = false
    if (numberOks == 0 && total == 2) {
      result = true
    }
    println("\n\n\nrisksVoidMatrixFileTest numero numberOks: " + numberOks + " y total: " + total)
    result
  }

  def risksVoidDeltaFxFileTest(numberOks: Long, total: Long): Boolean = {

    var result = false
    if (numberOks == 0 && total == 3) {
      result = true
    }
    println("\n\n\nrisksVoidMatrixFileTest numero numberOks: " + numberOks + " y total: " + total)
    result
  }

  def test: Boolean = {
    true
  }

}
