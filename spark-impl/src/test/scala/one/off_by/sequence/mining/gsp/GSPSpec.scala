package one.off_by.sequence.mining.gsp

import one.off_by.testkit.SparkTestBase
import org.scalatest.{Inspectors, Matchers, WordSpec}

class GSPSpec extends WordSpec
  with Matchers
  with Inspectors
  with SparkTestBase {

  "GSP" should {
    "have execute method" which {
    }

    "have prepareInitialPatterns method" which {
      "returns empty RDD for empty RDD" in withGSP { gsp =>
        val input = sc.parallelize(List[(SequenceId, gsp.TransactionType)]())
        val minSupportCount = 0L

        gsp.prepareInitialPatterns(input, minSupportCount) should be(empty)
      }

      "returns all transactions if min support is zero" in withGSP { gsp =>
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

        result should have length 3
        forAll(result.map(_._2)) { support =>
          support shouldBe 1L
        }
        val patterns = List(firstItem :: Nil, secondItem :: Nil, thirdItem :: Nil)
        result.map(_._1) should contain theSameElementsAs patterns
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

        println(gsp.prepareInitialPatterns(input, minSupportCount).toDebugString)
        val result = gsp.prepareInitialPatterns(input, minSupportCount).collect()

        result should contain theSameElementsAs List(
          (secondItem :: Nil, 2L),
          (thirdItem :: Nil, 3L)
        )
      }
    }

    "have generateJoinCandidates method" which {

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
