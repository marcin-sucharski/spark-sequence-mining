package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.Domain.{Element, Pattern}
import one.off_by.testkit.DefaultPatternHasherHelper
import org.scalatest.{Matchers, WordSpec}

class PatternJoinerSpec extends WordSpec
  with Matchers
  with DefaultPatternHasherHelper {

  "PatternWithHashSupport" should {
    import PatternJoiner._
    import PatternHasher.PatternSupport

    val singleItemPattern: Pattern[Int] = Pattern(Vector(Element(1)))
    val singleElementMultipleItemsPattern: Pattern[Int] = Pattern(Vector(Element(1, 2)))
    val doubleElementsPattern: Pattern[Int] = Pattern(Vector(Element(1), Element(2)))
    val multipleItemElementsPattern: Pattern[Int] = Pattern(Vector(Element(1, 2, 3), Element(4, 5, 6)))

    "provide `prefixes` method" which {
      "returns correct result for single-item patterns" in withHasher[Int] { implicit hasher =>
        val prefixes = singleItemPattern.prefixes

        prefixes should contain theSameElementsAs List(
          PrefixResult(
            Pattern(Vector[Element[Int]]()).hash,
            JoinItemNewElement(1)
          ),
          PrefixResult(
            Pattern(Vector(Element[Int]())).hash,
            JoinItemExistingElement(1)
          )
        )
      }

      "returns correct result for single-element multiple-item patterns" in withHasher[Int] { implicit hasher =>
        val prefixes = singleElementMultipleItemsPattern.prefixes

        prefixes should contain theSameElementsAs List(
          PrefixResult(
            Pattern(Vector(Element(1))).hash,
            JoinItemExistingElement(2)
          ),
          PrefixResult(
            Pattern(Vector(Element(2))).hash,
            JoinItemExistingElement(1)
          )
        )
      }

      "returns correct result for longer patterns" in withHasher[Int] { implicit hasher =>
        val prefixes = doubleElementsPattern.prefixes

        prefixes should contain theSameElementsAs List(
          PrefixResult(
            Pattern(Vector(Element(1))).hash,
            JoinItemNewElement(2)
          )
        )
      }

      "returns correct result with multiple-items elements" in withHasher[Int] { implicit hasher =>
        val prefixes = multipleItemElementsPattern.prefixes

        prefixes should contain theSameElementsAs List(
          PrefixResult(
            Pattern(Vector(Element(1, 2, 3), Element(4, 5))).hash,
            JoinItemExistingElement(6)
          ),
          PrefixResult(
            Pattern(Vector(Element(1, 2, 3), Element(4, 6))).hash,
            JoinItemExistingElement(5)
          ),
          PrefixResult(
            Pattern(Vector(Element(1, 2, 3), Element(5, 6))).hash,
            JoinItemExistingElement(4)
          )
        )
      }
    }

    "provide `suffixes` method" which {
      "returns correct result for single-item patterns" in withHasher[Int] { implicit hasher =>
        val suffixes = singleItemPattern.suffixes

        suffixes should contain theSameElementsAs List(
          SuffixResult(
            Pattern(Vector[Element[Int]]()).hash,
            JoinItemNewElement(1)
          ),
          SuffixResult(
            Pattern(Vector(Element[Int]())).hash,
            JoinItemExistingElement(1)
          )
        )
      }

      "returns correct result for single-element multiple-item patterns" in withHasher[Int] { implicit hasher =>
        val suffixes = singleElementMultipleItemsPattern.suffixes

        suffixes should contain theSameElementsAs List(
          SuffixResult(
            Pattern(Vector(Element(1))).hash,
            JoinItemExistingElement(2)
          ),
          SuffixResult(
            Pattern(Vector(Element(2))).hash,
            JoinItemExistingElement(1)
          )
        )
      }

      "returns correct result for longer patterns" in withHasher[Int] { implicit hasher =>
        val suffixes = doubleElementsPattern.suffixes

        suffixes should contain theSameElementsAs List(
          SuffixResult(
            Pattern(Vector(Element(2))).hash,
            JoinItemNewElement(1)
          )
        )
      }

      "returns correct result with multiple-items elements" in withHasher[Int] { implicit hasher =>
        val suffixes = multipleItemElementsPattern.suffixes

        suffixes should contain theSameElementsAs List(
          SuffixResult(
            Pattern(Vector(Element(1, 2), Element(4, 5, 6))).hash,
            JoinItemExistingElement(3)
          ),
          SuffixResult(
            Pattern(Vector(Element(1, 3), Element(4, 5, 6))).hash,
            JoinItemExistingElement(2)
          ),
          SuffixResult(
            Pattern(Vector(Element(2, 3), Element(4, 5, 6))).hash,
            JoinItemExistingElement(1)
          )
        )
      }
    }
  }
}
