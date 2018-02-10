package one.off_by.sequence.mining.gsp.readers

import one.off_by.sequence.mining.gsp.Transaction
import one.off_by.testkit.SparkTestBase
import org.scalatest.{Matchers, WordSpec}

import scala.reflect.io.File

class TransactionCSVInputReaderSpec
  extends WordSpec
    with Matchers
    with SparkTestBase {

  "TransactionCSVReader" should {
    "correctly read input file" in {
      val input =
        """
          |1,1,a,b,c
          |1,2,b,c
          |1,4,x
          |2,5,123,122
          |2,7,5,6
          |3,10,11,12
          |3,20,1
        """.stripMargin

      val tmp = File.makeTemp()
      try {
        tmp.writeAll(input)

        val result = new TransactionCSVInputReader().read(sc, tmp.path).collect().toSet

        result shouldBe Set(
          Transaction(1, 1, Set("a", "b", "c")),
          Transaction(1, 2, Set("b", "c")),
          Transaction(1, 4, Set("x")),

          Transaction(2, 5, Set("123", "122")),
          Transaction(2, 7, Set("5", "6")),

          Transaction(3, 10, Set("11", "12")),
          Transaction(3, 20, Set("1"))
        )
      } finally {
        tmp.delete()
      }
    }
  }
}
