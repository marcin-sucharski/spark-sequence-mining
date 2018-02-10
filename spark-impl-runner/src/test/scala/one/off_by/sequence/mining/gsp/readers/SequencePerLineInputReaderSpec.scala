package one.off_by.sequence.mining.gsp.readers

import one.off_by.sequence.mining.gsp.Transaction
import one.off_by.testkit.SparkTestBase
import org.scalatest.{Matchers, WordSpec, WordSpecLike}

import scala.reflect.io.File

class SequencePerLineInputReaderSpec
  extends WordSpec
    with Matchers
    with SparkTestBase {

  "SequencePerLineInputReader" should {
    "correctly reads input file" in {
      val input =
        """
          |1 -1 1 -1 2 -1 3 -1
          |2 -1 1 2 3 -1 1 2 3 -1
          |3 -1 1 -1 1 -1 1 -1
        """.stripMargin

      val tmp = File.makeTemp()
      try {
        tmp.writeAll(input)

        val result = new SequencePerLineInputReader().read(sc, tmp.path).collect().toSet

        result shouldBe Set(
          Transaction(1, 1, Set("1")),
          Transaction(1, 2, Set("2")),
          Transaction(1, 3, Set("3")),

          Transaction(2, 1, Set("1", "2", "3")),
          Transaction(2, 2, Set("1", "2", "3")),

          Transaction(3, 1, Set("1")),
          Transaction(3, 2, Set("1")),
          Transaction(3, 3, Set("1"))
        )
      } finally {
        tmp.delete()
      }
    }
  }
}
