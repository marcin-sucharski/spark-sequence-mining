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
        Transaction("C2", 50, Set("Ringworld Engineers")))

      "works with example data" in {
        val gsp = new GSP[String, Int, Int, String](sc)
        val input = sc.parallelize(inputData)

        val result = gsp.execute(input, 1.0).collect()

        result.map(_._1) should contain theSameElementsAs List(
          Pattern(Vector(Element("Ringworld"))),
          Pattern(Vector(Element("Foundation"))),
          Pattern(Vector(Element("Ringworld Engineers"))),
          Pattern(Vector(Element("Ringworld"), Element("Ringworld Engineers"))),
          Pattern(Vector(Element("Foundation"), Element("Ringworld Engineers"))))
      }

      "works with longer sequences" in {
        val transactions = List[Transaction[Int, Int, Int]](
          Transaction(1, 10, Set(1)),
          Transaction(1, 20, Set(2)),
          Transaction(1, 30, Set(3)),
          Transaction(1, 40, Set(4)),

          Transaction(2, 10, Set(1)),
          Transaction(2, 20, Set(2)),
          Transaction(2, 30, Set(3)),
          Transaction(2, 40, Set(4)),

          Transaction(3, 10, Set(1)),
          Transaction(3, 20, Set(2)),
          Transaction(3, 30, Set(3)),
          Transaction(3, 40, Set(4)))

        val gsp = new GSP[Int, Int, Int, Int](sc)
        val input = sc.parallelize(transactions)

        gsp.execute(input, 1.0).collect().map(_._1) should contain (
          Pattern(Vector(Element(1), Element(2), Element(3), Element(4))))
      }

      "works for sequence with additional noise data" in {
        val pattern = Pattern(Vector(
          Element(989, 1289),
          Element(548, 1122),
          Element(1498, 306, 808, 1881, 3025),
          Element(960, 1670),
          Element(901)))

        val transactions = List[Transaction[Int, Int, Int]](
          Transaction(1, 1, Set(989, 1289)),
          Transaction(1, 2, Set(548, 1122)),
          Transaction(1, 3, Set(511, 1549, 409)),
          Transaction(1, 4, Set(1498, 306, 808, 1881, 3025)),
          Transaction(1, 5, Set(960, 1670)),
          Transaction(1, 6, Set(901)),

          Transaction(2, 1, Set(459)),
          Transaction(2, 2, Set(338, 1294, 275)),
          Transaction(2, 3, Set(58, 1049, 2638, 866)),
          Transaction(2, 4, Set(927, 1396, 2682, 1106)),

          Transaction(3, 1, Set(989, 1289)),
          Transaction(3, 2, Set(548, 1122)),
          Transaction(3, 4, Set(1498, 306, 808, 1881, 3025)),
          Transaction(3, 5, Set(960, 1670)),
          Transaction(3, 6, Set(408, 1359, 164, 1920, 888, 2097, 2406)),
          Transaction(3, 7, Set(901)),
          Transaction(3, 9, Set(81, 1222)),

          Transaction(4, 1, Set(821)),
          Transaction(4, 2, Set(412)),
          Transaction(4, 3, Set(785, 1433, 663, 1794)),
          Transaction(4, 4, Set(396, 764, 2506, 2688, 1670)),

          Transaction(5, 1, Set(989, 1289)),
          Transaction(5, 2, Set(354, 1619)),
          Transaction(5, 3, Set(548, 1122)),
          Transaction(5, 4, Set(1498, 306, 808, 1881, 3025)),
          Transaction(5, 5, Set(960, 1670)),
          Transaction(5, 6, Set(901)))

        val gsp = new GSP[Int, Int, Int, Int](sc)
        val input = sc.parallelize(transactions)

        val patterns = gsp.execute(input, 0.4).collect().map(_._1)

        List(
          Pattern(Vector(Element(989))),
          Pattern(Vector(Element(1289))),
          Pattern(Vector(Element(548))),
          Pattern(Vector(Element(1122))),
          Pattern(Vector(Element(1498))),
          Pattern(Vector(Element(306))),
          Pattern(Vector(Element(808))),
          Pattern(Vector(Element(1881))),
          Pattern(Vector(Element(3025))),
          Pattern(Vector(Element(960))),
          Pattern(Vector(Element(1670))),
          Pattern(Vector(Element(901))),

          Pattern(Vector(Element(989, 1289))),
          Pattern(Vector(Element(548, 1122))),
          Pattern(Vector(Element(1498, 306))),
          Pattern(Vector(Element(306, 808))),
          Pattern(Vector(Element(808, 1881))),
          Pattern(Vector(Element(1881, 3025))),
          Pattern(Vector(Element(960, 1670))),

          Pattern(Vector(Element(989), Element(548))),
          Pattern(Vector(Element(1289), Element(548))),
          Pattern(Vector(Element(989), Element(1122))),

          Pattern(Vector(Element(989, 1289), Element(548))),
          Pattern(Vector(Element(989), Element(548, 1122))),
          Pattern(Vector(Element(1289), Element(548, 1122))),
          Pattern(Vector(Element(989, 1289), Element(1122))),

          Pattern(Vector(Element(989, 1289), Element(548, 1122))),

          Pattern(Vector(Element(1498, 306, 808, 1881, 3025))),


          pattern
        ) foreach { p =>
          patterns should contain (p)
        }
      }
    }

    "have prepareInitialPatterns method" which {
      "returns empty RDD for empty RDD" in withGSP { gsp =>
        val input = sc.parallelize(List[(SequenceId, gsp.TransactionType)]()).partitionBy(gsp.partitioner)
        val minSupportCount = 0

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
        val minSupportCount = 1

        val result = gsp.prepareInitialPatterns(input, minSupportCount).collect()

        forAll(result.map(_._2)) { support =>
          support shouldBe 1
        }
        result.map(_._1) should contain theSameElementsAs List(
          Pattern(Vector(Element(1))),
          Pattern(Vector(Element(2))),
          Pattern(Vector(Element(3))),
          Pattern(Vector(Element(4))))
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
        val minSupportCount = 2

        val result = gsp.prepareInitialPatterns(input, minSupportCount).collect()

        result should contain theSameElementsAs List(
          (Pattern(Vector(Element(2))), 2L),
          (Pattern(Vector(Element(3))), 3L),
          (Pattern(Vector(Element(4))), 3L))
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
