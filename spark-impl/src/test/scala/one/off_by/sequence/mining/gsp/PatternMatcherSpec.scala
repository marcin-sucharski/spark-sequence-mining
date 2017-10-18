package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.PatternMatcher._
import one.off_by.testkit.SparkTestBase
import org.apache.spark.{HashPartitioner, Partitioner}
import org.scalatest._

class PatternMatcherSpec extends FreeSpec
  with Matchers
  with Inspectors
  with SparkTestBase {

  "PatternMatcher should" - {
    val sequences = List[Transaction[Int, Int, Int]](
      Transaction(1, 1, Set(2)),
      Transaction(1, 2, Set(3)),
      Transaction(1, 3, Set(2, 3)),
      Transaction(1, 4, Set(5)),

      Transaction(2, 1, Set(1)),
      Transaction(2, 2, Set(1, 2)),
      Transaction(2, 3, Set(3, 4, 5)),

      Transaction(3, 1, Set(1, 2, 3)),
      Transaction(3, 2, Set(1, 2, 3)),
      Transaction(3, 3, Set(1, 2, 3))
    ).map(t => (t.sequenceId, t))

    "have internal `searchableSequences` field which" - {
      "contains correct helper data for searching" in {
        val output = List[PatternMatcher.SearchableSequence[Int, Int, Int]](
          PatternMatcher.SearchableSequence(
            1,
            Map(
              2 -> Vector(1, 3),
              3 -> Vector(2, 3),
              5 -> Vector(4)
            )
          ),
          PatternMatcher.SearchableSequence(
            2,
            Map(
              1 -> Vector(1, 2),
              2 -> Vector(2),
              3 -> Vector(3),
              4 -> Vector(3),
              5 -> Vector(3)
            )
          ),
          PatternMatcher.SearchableSequence(
            3,
            Map(
              1 -> Vector(1, 2, 3),
              2 -> Vector(1, 2, 3),
              3 -> Vector(1, 2, 3)
            )
          )
        )

        val patternMatcher = new PatternMatcher[Int, Int, Int, Int](partitioner, sc.parallelize(sequences), None, 0L)

        patternMatcher.searchableSequences.collect() should contain theSameElementsAs output
      }
    }

    "have `filter` method which" - {
      "filters out patterns not conforming to minimum support" in {
        val input = List[Pattern[Int]](
          Pattern(Vector(Element(2), Element(3), Element(2))),
          Pattern(Vector(Element(3), Element(2), Element(5))),
          Pattern(Vector(Element(1), Element(1), Element(3))),
          Pattern(Vector(Element(3), Element(2, 3)))
        )

        val output = List[Pattern[Int]](
          Pattern(Vector(Element(2), Element(3), Element(2))),
          Pattern(Vector(Element(1), Element(1), Element(3))),
          Pattern(Vector(Element(3), Element(2, 3)))
        )

        val minSupport = 2L
        val patternMatcher = new PatternMatcher[Int, Int, Int, Int](
          partitioner,sc.parallelize(sequences), None, minSupport)

        patternMatcher.filter(sc.parallelize(input)).collect().map(_._1) should contain theSameElementsAs output
      }
    }
  }

  private val partitioner: Partitioner = new HashPartitioner(4)
}

class PatternMatcherCompanionSpec extends FreeSpec
  with Matchers {

  "PatternMatcher should" - {
    val typeSupport = GSPTypeSupport[Int, Int]((a, b) => b - a, _ - _, _ + _)

    "have companion object that" - {
      "defines SearchableSequence which" - {
        val searchableSequence = SearchableSequence[Int, Int, Int](
          1,
          Map(
            1 -> Vector(1, 2, 3),
            2 -> Vector(3, 5, 7)
          )
        )

        "provides 'findFirstOccurrence' method which" - {
          "returns first item occurrence time if it exists" in {
            searchableSequence.findFirstOccurrence(1) should contain (1)
            searchableSequence.findFirstOccurrence(2) should contain (3)
          }

          "returns None if item is not present in SearchableSequence" in {
            searchableSequence.findFirstOccurrence(3) shouldNot be (defined)
          }
        }

        "provides 'findFirstOccurrenceAfter' method which" - {
          "returns first item occurrence if time is before first occurrence" in {
            searchableSequence.findFirstOccurrenceAfter(0, 1) should contain (1)
          }

          "returns second occurrence if time is equal to first occurrence" in {
            searchableSequence.findFirstOccurrenceAfter(1, 1) should contain (2)
          }

          "returns next occurrence if time is in between" in {
            searchableSequence.findFirstOccurrenceAfter(4, 2) should contain (5)
          }

          "returns None if time if equal to last occurrence" in {
            searchableSequence.findFirstOccurrenceAfter(7, 2) shouldNot be (defined)
          }

          "returns None if time if after last occurrence" in {
            searchableSequence.findFirstOccurrenceAfter(10, 2) shouldNot be (defined)
          }

          "returns None if item is not present" in {
            searchableSequence.findFirstOccurrenceAfter(0, 5) shouldNot be (defined)
          }
        }
      }

      "have internal `matches` method which" - {
        val sequence = List[Transaction[Int, Int, Int]](
          Transaction(1, 10, Set(1, 2)),
          Transaction(1, 25, Set(4, 6)),
          Transaction(1, 45, Set(3)),
          Transaction(1, 50, Set(1, 2)),
          Transaction(1, 65, Set(3)),
          Transaction(1, 90, Set(2, 4)),
          Transaction(1, 95, Set(6))
        )

        "for specified transaction set" - {

          type MatchesFunction[ItemType] = Pattern[ItemType] => Boolean

          def withMatcher(options: Option[GSPOptions[Int, Int]])(f: MatchesFunction[Int] => Unit): Unit = {
            f(PatternMatcher.matches[Int, Int, Int, Int](
              _,
              PatternMatcher.buildSearchableSequence(sequence),
              options
            ))
          }

          "with no gsp options" - {
            val matching = List[Pattern[Int]](
              Pattern(Vector(Element(1, 2))),
              Pattern(Vector(Element(1))),
              Pattern(Vector(Element(1), Element(3))),
              Pattern(Vector(Element(4), Element(6))),
              Pattern(Vector(Element(3), Element(2), Element(3), Element(2))),
              Pattern(Vector(Element(1, 2), Element(1, 2), Element(6)))
            )
            val notMatching = List[Pattern[Int]](
              Pattern(Vector(Element(8))),
              Pattern(Vector(Element(1, 2), Element(7))),
              Pattern(Vector(Element(7), Element(1, 2))),
              Pattern(Vector(Element(1), Element(1), Element(1), Element(1), Element(1))),
              Pattern(Vector(Element(1, 2, 6)))
            )

            val emptyOptions = Some(GSPOptions[Int, Int](typeSupport))
            for (pattern <- matching) {
              s"correctly matches $pattern when options are None" in withMatcher(None) { matches =>
                matches(pattern) shouldBe true
              }

              s"correctly matches $pattern when options are empty" in withMatcher(emptyOptions) { matches =>
                matches(pattern) shouldBe true
              }
            }

            for (pattern <- notMatching) {
              s"does not match $pattern when options are None" in withMatcher(None) { matches =>
                matches(pattern) shouldBe false
              }

              s"does not match $pattern when options are empty" in withMatcher(emptyOptions) { matches =>
                matches(pattern) shouldBe false
              }
            }
          }

          "with specified window size" - {
            val options = Some(GSPOptions[Int, Int](typeSupport, windowSize = Some(15)))

            val matching = List[Pattern[Int]](
              Pattern(Vector(Element(1, 2), Element(4, 6))),
              Pattern(Vector(Element(1, 2), Element(2, 4))),
              Pattern(Vector(Element(1, 2, 4, 6))),
              Pattern(Vector(Element(1, 2, 4), Element(2, 4, 6))),
              Pattern(Vector(Element(1, 2, 4), Element(1, 2), Element(2, 4, 6)))
            )
            val notMatching = List[Pattern[Int]](
              Pattern(Vector(Element(1, 2, 4, 6, 3))),
              Pattern(Vector(Element(4, 6, 3))),
              Pattern(Vector(Element(4, 6), Element(3, 2, 4)))
            )

            for (pattern <- matching)
              s"matches $pattern" in withMatcher(options) { matches =>
                matches(pattern) shouldBe true
              }

            for (pattern <- notMatching)
              s"does not $pattern" in withMatcher(options) { matches =>
                matches(pattern) shouldBe false
              }
          }

          "with specified min gap" - {
            val options = Some(GSPOptions[Int, Int](typeSupport, minGap = Some(20)))

            val matching = List[Pattern[Int]](
              Pattern(Vector(Element(1, 2), Element(3))),
              Pattern(Vector(Element(1, 2), Element(2, 4))),
              Pattern(Vector(Element(1), Element(6))),
              Pattern(Vector(Element(4, 6), Element(3)))
            )
            val notMatching = List[Pattern[Int]](
              Pattern(Vector(Element(1, 2), Element(4, 6))),
              Pattern(Vector(Element(1), Element(4, 6))),
              Pattern(Vector(Element(2, 4), Element(6))),
              Pattern(Vector(Element(3), Element(1, 2)))
            )

            for (pattern <- matching)
              s"matches $pattern" in withMatcher(options) { matches =>
                matches(pattern) shouldBe true
              }

            for (pattern <- notMatching)
              s"does not match $pattern" in withMatcher(options) { matches =>
                matches(pattern) shouldBe false
              }
          }

          "with specified max gap" - {
            val options = Some(GSPOptions[Int, Int](typeSupport, maxGap = Some(40)))

            val matching = List[Pattern[Int]](
              Pattern(Vector(Element(1, 2), Element(3), Element(3), Element(6))),
              Pattern(Vector(Element(4, 6), Element(3), Element(2, 4))),
              Pattern(Vector(Element(6), Element(2), Element(4))),
              Pattern(Vector(Element(1, 2), Element(2, 4)))
            )
            val notMatching = List[Pattern[Int]](
              Pattern(Vector(Element(6), Element(6))),
              Pattern(Vector(Element(4, 6), Element(2, 4)))
            )

            for (pattern <- matching)
              s"matches $pattern" in withMatcher(options) { matches =>
                matches(pattern) shouldBe true
              }

            for (pattern <- notMatching)
              s"does not match $pattern" in withMatcher(options) { matches =>
                matches(pattern) shouldBe false
              }
          }

          "with all options specified" - {
            val options = Some(GSPOptions[Int, Int](
              typeSupport,
              windowSize = Some(10),
              minGap = Some(20),
              maxGap = Some(40)
            ))

            val matching = List[Pattern[Int]](
              Pattern(Vector(Element(1, 2), Element(3, 1, 2), Element(2, 4))),
              Pattern(Vector(Element(1, 2), Element(3, 1, 2), Element(2, 4, 6))),
              Pattern(Vector(Element(4, 6), Element(1, 2), Element(2, 4, 6))),
              Pattern(Vector(Element(1), Element(3), Element(3), Element(6))),
              Pattern(Vector(Element(1, 2), Element(4, 6)))
            )
            val notMatching = List[Pattern[Int]](
              Pattern(Vector(Element(1, 2, 4, 6))),
              Pattern(Vector(Element(1, 2, 4), Element(2, 4))),
              Pattern(Vector(Element(3), Element(1, 2)))
            )

            for (pattern <- matching)
              s"matches $pattern" in withMatcher(options) { matches =>
                matches(pattern) shouldBe true
              }

            for (pattern <- notMatching)
              s"does not match $pattern" in withMatcher(options) { matches =>
                matches(pattern) shouldBe false
              }
          }
        }
      }

      "defines ElementFinder trait that" - {
        val searchableSequence = PatternMatcher.buildSearchableSequence(
          List[Transaction[Int, Int, Int]](
            Transaction(1, 10, Set(1, 2)),
            Transaction(1, 25, Set(4, 6)),
            Transaction(1, 45, Set(3)),
            Transaction(1, 65, Set(1, 2)),
            Transaction(1, 90, Set(8, 9))
          )
        )

        val zero = MinTime(0, inclusive = true)

        "has SimpleElementFinder implementation which" - {
          def withFinder(f: ElementFinder[Int, Int] => Unit): Unit =
            f(new SimpleElementFinder[Int, Int, Int](searchableSequence))

          "implements `find` method which" - {
            "finds only subsets of real elements" in withFinder { finder =>
              finder.find(zero, Element(1, 2)) should contain ((10, 10))
              finder.find(zero, Element(2)) should contain ((10, 10))
              finder.find(zero, Element(1)) should contain ((10, 10))
              finder.find(zero, Element(4, 6)) should contain ((25, 25))
              finder.find(zero, Element(6)) should contain ((25, 25))
              finder.find(zero, Element(3)) should contain ((45, 45))
            }

            "finds elements with minTime" in withFinder { finder =>
              finder.find(MinTime(40, inclusive = true), Element(1, 2)) should contain ((65, 65))
              finder.find(MinTime(40, inclusive = true), Element(1)) should contain ((65, 65))
              finder.find(MinTime(40, inclusive = true), Element(2)) should contain ((65, 65))
            }

            "does not merge elements" in withFinder { finder =>
              finder.find(zero, Element(1, 2, 4, 6)) shouldNot be (defined)
            }

            "does not find elements before specified time" in withFinder { finder =>
              finder.find(MinTime(70, inclusive = true), Element(3)) shouldNot be (defined)
            }

            "does not find not existing elements" in withFinder { finder =>
              finder.find(zero, Element(10)) shouldNot be (defined)
            }

            "correctly handles inclusive minTime" in withFinder { finder =>
              finder.find(MinTime(10, inclusive = true), Element(1, 2)) should contain ((10, 10))
            }

            "correctly handles non-inclusive minTime" in withFinder { finder =>
              finder.find(MinTime(10, inclusive = false), Element(1, 2)) should contain ((65, 65))
            }
          }
        }

        "has SlidingWindowElementFinder implementation which" - {
          "implements `find` method which" - {
            val windowSize = 20

            def withFinder(f: ElementFinder[Int, Int] => Unit): Unit =
              f(new SlidingWindowElementFinder[Int, Int, Int, Int](searchableSequence, windowSize, typeSupport))

            "finds subsets of real elements" in withFinder { finder =>
              finder.find(zero, Element(1, 2)) should contain ((10, 10))
              finder.find(zero, Element(1)) should contain ((10, 10))
              finder.find(zero, Element(2)) should contain ((10, 10))
            }

            "finds elements with specified minTime" in withFinder { finder =>
              finder.find(MinTime(20, inclusive = true), Element(1, 2)) should contain ((65, 65))
              finder.find(MinTime(20, inclusive = true), Element(1)) should contain ((65, 65))
              finder.find(MinTime(20, inclusive = true), Element(2)) should contain ((65, 65))
            }

            "finds elements spanning multiple real elements" in withFinder { finder =>
              finder.find(zero, Element(1, 2, 4, 6)) should contain ((10, 25))
              finder.find(zero, Element(3, 1, 2)) should contain ((45, 65))
            }

            "returns None for elements spanning multiple real elements not within window size" in withFinder { finder =>
              finder.find(zero, Element(2, 8, 9)) shouldNot be (defined)
              finder.find(zero, Element(3, 8, 9)) shouldNot be (defined)
            }

            "returns None for elements with items not in transaction set" in withFinder { finder =>
              finder.find(zero, Element(10)) shouldNot be (defined)
            }

            "correctly handles inclusive minTime" in withFinder { finder =>
              finder.find(MinTime(10, inclusive = true), Element(1, 2)) should contain ((10, 10))
            }

            "correctly handles non-inclusive minTime" in withFinder { finder =>
              finder.find(MinTime(10, inclusive = false), Element(1, 2)) should contain ((65, 65))
            }
          }
        }
      }
    }
  }
}
