package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.Domain.{Element, Pattern}
import one.off_by.testkit.DefaultPatternHasherHelper
import org.scalatest.{Matchers, WordSpec}

class DefaultPatternHasherSpec extends WordSpec
  with Matchers
  with DefaultPatternHasherHelper {

  "DefaultPatternHasher" should {
    "provide `nil` method" which {
      "returns always the same value" in withHasher[Int] { hasher =>
        hasher.nil shouldBe hasher.nil
      }
    }

    "provide `hash` method" which {
      "returns `nil` for empty pattern" in withHasher[Int] { hasher =>
        hasher.hash(Pattern(Vector[Element[Int]]())) shouldBe hasher.nil
      }

      "returns different values for different patterns" in withHasher[Int] { hasher =>
        val first = Pattern(Vector(Element(1, 2), Element(3)))
        val second = Pattern(Vector(Element(1), Element(2, 3)))

        hasher.hash(first) shouldNot be(hasher.hash(second))
      }

      "returns different values for patterns with elements in different order" in withHasher[Int] { hasher =>
        val first = Pattern(Vector(Element(1, 2), Element(3)))
        val second = Pattern(Vector(Element(3), Element(1, 2)))

        hasher.hash(first) shouldNot be(hasher.hash(second))
      }
    }

    "provide `appendRight` method" which {
      val element = Element(5, 6)
      val prefixElements = Vector(Element(1, 2), Element(2, 3))

      "for single element returns same pattern as hash" in withHasher[Int] { hasher =>
        val pattern = Pattern(Vector(element))

        hasher.appendRight(hasher.nil, element) shouldBe hasher.hash(pattern)
      }

      "correctly extends pattern with an element" in withHasher[Int] { hasher =>
        val initialPattern = Pattern(prefixElements)
        val extendedPattern = Pattern(prefixElements :+ element)

        hasher.appendRight(hasher.hash(initialPattern), element) shouldBe hasher.hash(extendedPattern)
      }
    }
  }
}
