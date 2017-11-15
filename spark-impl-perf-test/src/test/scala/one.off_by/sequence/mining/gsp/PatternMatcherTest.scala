package one.off_by.sequence.mining.gsp

import org.scalameter.api._
import org.scalameter.picklers.Implicits._

class PatternMatcherBenchmark extends Bench.ForkedTime {

  performance of "PatternMatcher" in {
    import one.off_by.sequence.mining.gsp.PatternMatcherTest.options
    import PatternMatcherTest.{genericSequence, longSequence}

    measure method "matches" in {
      performance of "GenericSequence" in {
        using(Gen.crossProduct(options.possibleOptions, genericSequence.patterns)) in { case (opt, patterns) =>
          (0 /: patterns)(_ + matches(_, genericSequence.searchableSequence, opt))
        }
      }

      performance of "LongSequence" in {
        using(Gen.crossProduct(options.possibleOptions, longSequence.patterns)) in { case (opt, patterns) =>
          (0 /: patterns)(_ + matches(_, longSequence.searchableSequence, opt))
        }
      }
    }
  }

  @inline
  private def matches(
    pattern: Pattern[Int],
    searchableSequence: PatternMatcher.SearchableSequence[Int, Int, Int],
    options: Option[PatternMatcherTest.options.OptionsType]): Int =
    if (PatternMatcher.matches(pattern, searchableSequence, options)) 1 else 0

}

private object PatternMatcherTest {
  object genericSequence {
    private val sequence: List[Transaction[Int, Int, Int]] = List[Transaction[Int, Int, Int]](
      Transaction(1, 10, Set(1, 2)),
      Transaction(1, 25, Set(4, 6)),
      Transaction(1, 45, Set(3)),
      Transaction(1, 50, Set(1, 2)),
      Transaction(1, 65, Set(3)),
      Transaction(1, 90, Set(2, 4)),
      Transaction(1, 95, Set(6))
    )

    val searchableSequence: PatternMatcher.SearchableSequence[Int, Int, Int] =
      PatternMatcher.buildSearchableSequence(sequence)

    private val sourcePatterns = Vector(
      Pattern(Vector(Element(1, 2))),
      Pattern(Vector(Element(1))),
      Pattern(Vector(Element(1), Element(3))),
      Pattern(Vector(Element(4), Element(6))),
      Pattern(Vector(Element(3), Element(2), Element(3), Element(2))),
      Pattern(Vector(Element(1, 2), Element(1, 2), Element(6))),
      Pattern(Vector(Element(8))),
      Pattern(Vector(Element(1, 2), Element(7))),
      Pattern(Vector(Element(7), Element(1, 2))),
      Pattern(Vector(Element(1), Element(1), Element(1), Element(1), Element(1))),
      Pattern(Vector(Element(1, 2, 6))),
      Pattern(Vector(Element(1, 2), Element(3, 1, 2), Element(2, 4))),
      Pattern(Vector(Element(1, 2), Element(3, 1, 2), Element(2, 4, 6))),
      Pattern(Vector(Element(4, 6), Element(1, 2), Element(2, 4, 6))),
      Pattern(Vector(Element(1), Element(3), Element(3), Element(6))),
      Pattern(Vector(Element(1, 2), Element(4, 6))),
      Pattern(Vector(Element(1, 2, 4, 6))),
      Pattern(Vector(Element(1, 2, 4), Element(2, 4))),
      Pattern(Vector(Element(3), Element(1, 2))),
      Pattern(Vector(Element(6), Element(6))),
      Pattern(Vector(Element(4, 6), Element(2, 4))),
      Pattern(Vector(Element(1, 2), Element(3), Element(3), Element(6))),
      Pattern(Vector(Element(4, 6), Element(3), Element(2, 4))),
      Pattern(Vector(Element(6), Element(2), Element(4))),
      Pattern(Vector(Element(1, 2), Element(2, 4))),
      Pattern(Vector(Element(1, 2), Element(4, 6))),
      Pattern(Vector(Element(1), Element(4, 6))),
      Pattern(Vector(Element(2, 4), Element(6))),
      Pattern(Vector(Element(3), Element(1, 2))),
      Pattern(Vector(Element(1, 2), Element(3))),
      Pattern(Vector(Element(1, 2), Element(2, 4))),
      Pattern(Vector(Element(1), Element(6))),
      Pattern(Vector(Element(4, 6), Element(3))),
      Pattern(Vector(Element(1, 2, 4, 6, 3))),
      Pattern(Vector(Element(4, 6, 3))),
      Pattern(Vector(Element(4, 6), Element(3, 2, 4))),
      Pattern(Vector(Element(1, 2), Element(4, 6))),
      Pattern(Vector(Element(1, 2), Element(2, 4))),
      Pattern(Vector(Element(1, 2, 4, 6))),
      Pattern(Vector(Element(1, 2, 4), Element(2, 4, 6))),
      Pattern(Vector(Element(1, 2, 4), Element(1, 2), Element(2, 4, 6))))

    private val infinitePatterns = Stream.continually(sourcePatterns).flatten

    val patterns: Gen[IndexedSeq[Pattern[Int]]] = {
      val thousand = 1000

      val start = 10 * thousand
      val stop = 100 * thousand
      val step = 30 * thousand

      Gen.range("size")(start, stop, step) map { size =>
        infinitePatterns.take(size).toIndexedSeq
      }
    }
  }

  object longSequence {
    private val sequenceLength = 100 * 1000
    private val moduloItems = 7
    private val availableItems = List(1, 2, 3, 4, 5, 6, 7)
    private val itemsSource = Stream.continually(availableItems).flatten

    private val sequence = 1 to sequenceLength map { i =>
      Transaction[Int, Int, Int](1, i * 5, itemsSource.take(i % moduloItems + 1).toSet)
    }

    val searchableSequence: PatternMatcher.SearchableSequence[Int, Int, Int] =
      PatternMatcher.buildSearchableSequence(sequence)

    private val infinitePatterns = Stream.continually(1 to 10 map (_ => availableItems)).zipWithIndex
      .map { case (itemsets, index) =>
        val itemCountBase = index % availableItems.length + 1
        val mappedItemsets = itemsets.zipWithIndex map { case (items, itemsetIndex) =>
          val itemCount = (itemCountBase * (itemsetIndex + 1)) % availableItems.length + 1
          Element(items.reverse.take(itemCount): _*)
        }
        Pattern(mappedItemsets.toVector)
      }

    val patterns: Gen[IndexedSeq[Pattern[Int]]] = {
      val thousand = 1000

      val start = 1 * thousand
      val stop = 10 * thousand
      val step = 3 * thousand

      Gen.range("size")(start, stop, step) map { size =>
        infinitePatterns.take(size).toIndexedSeq
      }
    }
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
