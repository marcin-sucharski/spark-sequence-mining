package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.Domain.{Element, Pattern}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}

class PatternHasherSupportSpec extends WordSpec
  with Matchers
  with MockFactory {

  "HashSupport" should {
    import PatternHasher.HashSupport

    "provide ##: method" which {
      "is shortcut for `appendLeft`" in {
        val element = Element(1, 2)
        val hash = Hash[Int](1)
        val newHash = Hash[Int](2)

        implicit val hasher: PatternHasher[Int] = mock[PatternHasher[Int]]
        (hasher.appendRight _).expects(hash, element).returns(newHash)

        hash :## element shouldBe newHash
      }
    }
  }

  "PatternSupport" should {
    import PatternHasher.PatternSupport

    "provide `rawHash` method" which {
      "returns hash for specified pattern" in {
        val pattern = Pattern(Vector(Element(1, 2), Element(3, 4)))
        val hash = Hash[Int](1)

        implicit val hasher: PatternHasher[Int] = mock[PatternHasher[Int]]
        (hasher.hash(_: Pattern[Int])) expects pattern returns hash

        pattern.rawHash shouldBe hash
      }
    }

    "provider `hash` method" which {
      val element_1 = Element(1, 2)
      val element_2 = Element(3)
      val element_3 = Element(4, 5)

      implicit val hasher: PatternHasher[Int] = new DefaultPatternHasher[Int]()

      "returns PatternWithHash for specified pattern" in {
        val pattern = Pattern(Vector(element_1, element_2, element_3))

        val patternWithHash = pattern.hash

        patternWithHash.pattern shouldBe pattern
        patternWithHash.hash shouldBe hasher.hash(pattern)
      }

      "returns PatternWithHash for pattern with length of 2" in {
        val pattern = Pattern(Vector(element_1, element_2))

        val patternWithHash = pattern.hash

        patternWithHash.pattern shouldBe pattern
        patternWithHash.hash shouldBe hasher.hash(pattern)
      }

      "returns PatternWithHash for pattern with length of 1" in {
        val pattern = Pattern(Vector(element_1))

        val patternWithHash = pattern.hash

        patternWithHash.pattern shouldBe pattern
        patternWithHash.hash shouldBe hasher.hash(pattern)
      }
    }
  }
}
