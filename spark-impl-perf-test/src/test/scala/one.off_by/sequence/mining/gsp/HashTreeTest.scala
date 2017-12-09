package one.off_by.sequence.mining.gsp

import org.scalameter.{Bench, Gen}
import org.scalameter.picklers.Implicits._

class HashTreeTest extends Bench.ForkedTime {
  import HashTreeTest._
  import options.possibleOptions

  performance of "HashTree" in {
    measure method "findPossiblePatterns" in {
      using(Gen.crossProduct(sequenceGen, hashTreeGen, possibleOptions)) in { case (sequence, hashTree, options) =>
        hashTree.findPossiblePatterns(options, sequence).toList
      }
    }
  }
}

private object HashTreeTest {
  val size = 200

  val sequenceGen: Gen[List[Transaction[Int, Int, Int]]] = Gen.single("sequence")(1) map { _ =>
    (1 to size sliding 4).zipWithIndex.toList map { case (seq, i) =>
      Transaction(1, i, seq.toSet)
    }
  }

  val candidatePatterns: Seq[Pattern[Int]] =
    (for {
      x <- 1 to size * 3
      y <- 1 to size * 3
      if x != y
    } yield Pattern(Vector(Element(x), Element(y))) :: Pattern(Vector(Element(x, y))) :: Nil).flatten.distinct

  val hashTreeGen: Gen[HashTree[Int, Int, Int, Int]] = Gen.single("hashTree")(1) map { _ =>
    ((HashTreeLeaf[Int, Int, Int, Int](): HashTree[Int, Int, Int, Int]) /: candidatePatterns)(_ add _)
  }

  object options {
    type OptionsType = GSPOptions[Int, Int]
    val typeSupport: GSPTypeSupport[Int, Int] = GSPTypeSupport[Int, Int]((a, b) => b - a, _ - _, _ + _)

    val none: Option[OptionsType] = None
    val withWindowSize: Option[OptionsType] = Some(GSPOptions[Int, Int](typeSupport, windowSize = Some(15)))
    val withMinGap: Option[OptionsType] = Some(GSPOptions[Int, Int](typeSupport, minGap = Some(20)))
    val withMaxGap: Option[OptionsType] = Some(GSPOptions[Int, Int](typeSupport, maxGap = Some(40)))
    val withAll: Option[OptionsType] = Some(GSPOptions[Int, Int](
      typeSupport,
      windowSize = Some(10),
      minGap = Some(20),
      maxGap = Some(40)))

    private val optionsMap = Map(
      "none" -> none,
      "withWindowSize" -> withWindowSize,
      "withMinGap" -> withMinGap,
      "withMaxGap" -> withMaxGap,
      "withAll" -> withAll)

    val possibleOptions: Gen[Option[OptionsType]] =
      Gen.enumeration("options")(optionsMap.keys.toSeq: _*) map (optionsMap(_))
  }
}
