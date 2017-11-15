package one.off_by.sequence.mining.gsp

import one.off_by.testkit.SparkTestBase
import org.scalatest.{Inspectors, Matchers, WordSpec}

class GSPSpec extends WordSpec
  with Matchers
  with Inspectors
  with SparkTestBase {

  "GSP" should {
    "have execute method" which {
      val inputData = List[Transaction[String, Int, String]](
        Transaction("C1", 1, Set("Ringworld")),
        Transaction("C1", 2, Set("Foundation")),
        Transaction("C1", 15, Set("Ringworld Engineers", "Second Foundation")),
        Transaction("C2", 1, Set("Foundation", "Ringworld")),
        Transaction("C2", 20, Set("Foundation and Empire")),
        Transaction("C2", 50, Set("Ringworld Engineers"))
      )

      "works with example data" in {
        val gsp = new GSP[String, Int, Int, String](sc)
        val input = sc.parallelize(inputData)

        val result = gsp.execute(input, 1.0).collect()

        result.map(_._1) should contain theSameElementsAs List(
          Pattern(Vector(Element("Ringworld"))),
          Pattern(Vector(Element("Foundation"))),
          Pattern(Vector(Element("Ringworld Engineers"))),
          Pattern(Vector(Element("Ringworld"), Element("Ringworld Engineers"))),
          Pattern(Vector(Element("Foundation"), Element("Ringworld Engineers")))
        )
      }
    }

    "have prepareInitialPatterns method" which {
      "returns empty RDD for empty RDD" in withGSP { gsp =>
        val input = sc.parallelize(List[(SequenceId, gsp.TransactionType)]()).partitionBy(gsp.partitioner)
        val minSupportCount = 0L

        gsp.prepareInitialPatterns(input, minSupportCount) should be(empty)
      }

      "returns all transactions if min support is one" in withGSP { gsp =>
        val firstItem = Set(1)
        val secondItem = Set(2)
        val thirdItem = Set(3, 4)
        val input = sc.parallelize(List[(SequenceId, gsp.TransactionType)](
          (1, Transaction(1, 1, firstItem)),
          (2, Transaction(2, 2, secondItem)),
          (3, Transaction(3, 3, thirdItem))
        )).partitionBy(gsp.partitioner)
        val minSupportCount = 1L

        val result = gsp.prepareInitialPatterns(input, minSupportCount).collect()

        forAll(result.map(_._2)) { support =>
          support shouldBe 1L
        }
        result.map(_._1) should contain theSameElementsAs List(
          Pattern(Vector(Element(1))),
          Pattern(Vector(Element(2))),
          Pattern(Vector(Element(3))),
          Pattern(Vector(Element(4)))
        )
      }

      "do not return initial patterns with support below min support" in withGSP { gsp =>
        val firstItem = Set(1)
        val secondItem = Set(2)
        val thirdItem = Set(3, 4)
        val input = sc.parallelize(List[(SequenceId, gsp.TransactionType)](
          (1, Transaction(1, 1, firstItem)),

          (2, Transaction(2, 2, secondItem)),
          (3, Transaction(3, 3, secondItem)),
          (3, Transaction(3, 4, secondItem)),

          (4, Transaction(4, 4, thirdItem)),
          (5, Transaction(5, 5, thirdItem)),
          (6, Transaction(6, 6, thirdItem))
        )).partitionBy(gsp.partitioner)
        val minSupportCount = 2L

        val result = gsp.prepareInitialPatterns(input, minSupportCount).collect()

        result should contain theSameElementsAs List(
          (Pattern(Vector(Element(2))), 2L),
          (Pattern(Vector(Element(3))), 3L),
          (Pattern(Vector(Element(4))), 3L)
        )
      }
    }
  }

  private type ItemType = Int
  private type DurationType = Int
  private type TimeType = Int
  private type SequenceId = Int

  private def withGSP[T](f: GSP[ItemType, DurationType, TimeType, SequenceId] => T): T = {
    f(new GSP[ItemType, DurationType, TimeType, SequenceId](sc))
  }
}
