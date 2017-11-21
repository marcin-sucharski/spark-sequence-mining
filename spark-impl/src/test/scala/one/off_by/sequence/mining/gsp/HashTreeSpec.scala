package one.off_by.sequence.mining.gsp

import org.scalatest.{FreeSpec, Matchers}

class HashTreeSpec extends FreeSpec
  with Matchers {

  "HashTree should" - {
    val empty = HashTree.empty[Int, Int, Int, Int]
    val typeSupport = GSPTypeSupport[Int, Int](
      (a, b) => b - a,
      (a, b) => a - b,
      (a, b) => a + b)

    "not contain any patterns for sequence if is empty" in {
      empty.findPossiblePatterns(None, Seq(Transaction(1, 1, Set(1)))) shouldBe 'empty
    }

    "returns patterns which are supported by sequence when" - {
      val patterns = List(
        Pattern(Vector(Element(1), Element(2, 3, 4))),
        Pattern(Vector(Element(3), Element(4, 1, 2))),
        Pattern(Vector(Element(1, 2), Element(3, 4))),
        Pattern(Vector(Element(1, 2, 3), Element(4))),
        Pattern(Vector(Element(1), Element(2), Element(3, 4))))

      val transactions = List(
        Transaction(1, 10, Set(1)),
        Transaction(1, 15, Set(1, 2)),
        Transaction(1, 20, Set(2)),
        Transaction(1, 25, Set(2, 3, 4)),
        Transaction(1, 30, Set(3, 4)))

      val hashTreeWithPatterns = (empty /: patterns)(_ add _)

      "no options are specified" in {
        hashTreeWithPatterns.findPossiblePatterns(None, transactions) should contain allOf(
          Pattern(Vector(Element(1), Element(2, 3, 4))),
          Pattern(Vector(Element(1, 2), Element(3, 4))),
          Pattern(Vector(Element(1), Element(2), Element(3, 4))))
      }

      "window size is specified" in {
        val options = Some(GSPOptions(typeSupport, windowSize = Some(10)))

        hashTreeWithPatterns.findPossiblePatterns(options, transactions) should contain allOf(
          Pattern(Vector(Element(1), Element(2, 3, 4))),
          Pattern(Vector(Element(1, 2), Element(3, 4))),
          Pattern(Vector(Element(1, 2, 3), Element(4))),
          Pattern(Vector(Element(1), Element(2), Element(3, 4))))
      }

      "max gap is specified" in {
        val options = Some(GSPOptions(typeSupport, maxGap = Some(5)))

        hashTreeWithPatterns.findPossiblePatterns(options, transactions) should contain (
          Pattern(Vector(Element(1), Element(2), Element(3, 4))))
      }

      "window size and max gap are specified" in {
        val options = Some(GSPOptions(typeSupport, windowSize = Some(10), maxGap = Some(5)))

        hashTreeWithPatterns.findPossiblePatterns(options, transactions) should contain allOf(
          Pattern(Vector(Element(1), Element(2, 3, 4))),
          Pattern(Vector(Element(1, 2), Element(3, 4))),
          Pattern(Vector(Element(1, 2, 3), Element(4))),
          Pattern(Vector(Element(1), Element(2), Element(3, 4)))
        )
      }

      "trivial case of 2-length sequences is given" in {
        val patterns = List[Pattern[Int]](
          Pattern(Vector(Element(1), Element(1))),
          Pattern(Vector(Element(3), Element(1))),
          Pattern(Vector(Element(2), Element(1))),
          Pattern(Vector(Element(1), Element(3))),
          Pattern(Vector(Element(3), Element(3))),
          Pattern(Vector(Element(2), Element(3))),
          Pattern(Vector(Element(1), Element(2))),
          Pattern(Vector(Element(3), Element(2))),
          Pattern(Vector(Element(2), Element(2))),
          Pattern(Vector(Element(4), Element(4))),
          Pattern(Vector(Element(4), Element(1))),
          Pattern(Vector(Element(4), Element(3))),
          Pattern(Vector(Element(4), Element(2))),
          Pattern(Vector(Element(1), Element(4))),
          Pattern(Vector(Element(3), Element(4))),
          Pattern(Vector(Element(2), Element(4))),
          Pattern(Vector(Element(4, 1))),
          Pattern(Vector(Element(4, 3))),
          Pattern(Vector(Element(4, 2))),
          Pattern(Vector(Element(3, 1))),
          Pattern(Vector(Element(2, 1))),
          Pattern(Vector(Element(3, 2))))

        val sequence = List[Transaction[Int, Int, Int]](
          Transaction(1, 10, Set(1)),
          Transaction(1, 20, Set(2)),
          Transaction(1, 30, Set(3)),
          Transaction(1, 40, Set(4)))

        val hashTree = (HashTree.empty[Int, Int, Int, Int] /: patterns)(_ add _)

        hashTree.findPossiblePatterns(None, sequence) should contain allOf(
          Pattern(Vector(Element(1), Element(2))),
          Pattern(Vector(Element(1), Element(3))),
          Pattern(Vector(Element(1), Element(4))),

          Pattern(Vector(Element(2), Element(3))),
          Pattern(Vector(Element(2), Element(4))),

          Pattern(Vector(Element(3), Element(4))))
      }
    }
  }
}
