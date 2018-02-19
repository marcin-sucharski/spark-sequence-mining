package one.off_by.sequence.mining.gsp

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole


@State(Scope.Benchmark)
class PatternMatcherBenchmark {
  @Param(Array("none", "withWindowSize", "withMinGap", "withMaxGap", "withAll"))
  var optionsMode: String = _

  @Param(Array("genericSequence", "longSequence"))
  var sequenceType: String = _

  @Setup
  def setup(): Unit = {
    options = PatternMatcherTest.options.optionsMap(optionsMode)

    if (sequenceType == "genericSequence") {
      patterns = PatternMatcherTest.genericSequence.patterns
      searchableSequence = PatternMatcherTest.genericSequence.searchableSequence
    }
    else if (sequenceType == "longSequence") {
      patterns = PatternMatcherTest.longSequence.patterns
      searchableSequence = PatternMatcherTest.longSequence.searchableSequence
    }
    else sys.error("incorrect sequence type")
  }

  private var options: Option[PatternMatcherTest.options.OptionsType] = _
  private var searchableSequence: PatternMatcher.SearchableSequence[Int, Int, Int] = _
  private var patterns: Vector[Pattern[Int]] = _

  @Benchmark
  def measureMatchesPerformance(bh: Blackhole): Unit = {
    var i = 0
    while (i < patterns.size) {
      bh.consume(PatternMatcher.matches(patterns(i), searchableSequence, options))
      i += 1
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

    val patterns = Vector(
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

    val patterns: Vector[Pattern[Int]] = infinitePatterns.take(50).toVector
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

    val optionsMap = Map(
      "none" -> none,
      "withWindowSize" -> withWindowSize,
      "withMinGap" -> withMinGap,
      "withMaxGap" -> withMaxGap,
      "withAll" -> withAll)
  }

}
