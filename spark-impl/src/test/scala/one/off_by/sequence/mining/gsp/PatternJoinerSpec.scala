package one.off_by.sequence.mining.gsp

import one.off_by.testkit.SparkTestBase
import org.apache.spark.{HashPartitioner, Partitioner}
import org.scalatest.{Inspectors, Matchers, WordSpec}

class PatternJoinerSpec extends WordSpec
  with Matchers
  with Inspectors
  with SparkTestBase {

  "PatternJoiner" should {
    val sourcePatterns = List(
      Pattern(Vector(Element(1, 2), Element(3))),
      Pattern(Vector(Element(1, 2), Element(4))),
      Pattern(Vector(Element(1), Element(3, 4))),
      Pattern(Vector(Element(1, 3), Element(5))),
      Pattern(Vector(Element(2), Element(3, 4))),
      Pattern(Vector(Element(2), Element(3), Element(5))))
    val afterJoinPatterns = List(
      Pattern(Vector(Element(1, 2), Element(3, 4))),
      Pattern(Vector(Element(1, 2), Element(3), Element(5))))
    val afterPruningPatterns = List(
      Pattern(Vector(Element(1, 2), Element(3, 4))))

    val sourceSingleItemPatterns = List(
      Pattern(Vector(Element(1))),
      Pattern(Vector(Element(2))),
      Pattern(Vector(Element(3))))
    val afterJoinSingleItemPatterns = List(
      Pattern(Vector(Element(1), Element(1))),
      Pattern(Vector(Element(1), Element(2))),
      Pattern(Vector(Element(1), Element(3))),
      Pattern(Vector(Element(2), Element(2))),
      Pattern(Vector(Element(2), Element(3))),
      Pattern(Vector(Element(2), Element(1))),
      Pattern(Vector(Element(3), Element(1))),
      Pattern(Vector(Element(3), Element(2))),
      Pattern(Vector(Element(3), Element(3))),
      Pattern(Vector(Element(1, 2))),
      Pattern(Vector(Element(2, 3))),
      Pattern(Vector(Element(1, 3))))
    val afterPruneSingleItemPatterns = afterJoinSingleItemPatterns

    "provide `generateCandidates` method" which {
      "generates correct candidates set with join and prune" in withPatternJoiner[Int] { joiner =>
        val source = sc.parallelize(sourcePatterns)

        val result = joiner.generateCandidates(source).collect()

        result should contain theSameElementsAs afterPruningPatterns
      }

      "generate correct candidates with defined max gap and window size" in {
        val gspOptions = GSPOptions(
          typeSupport = GSPTypeSupport[Int, Int](
            timeDistance = (a, b) => b - a,
            timeSubtract = (a, b) => a - b,
            timeAdd = (a, b) => a + b
          ),
          windowSize = Some(10),
          maxGap = Some(10)
        )

        val joiner = new PatternJoiner[Int](partitioner, Some(gspOptions))
        // because of time constraints it is possible that <(1)(3)(4)> won't be frequent pattern
        val source = sc.parallelize(List(
          Pattern(Vector(Element(1), Element(2, 3))),
          Pattern(Vector(Element(2, 3), Element(4))),
          Pattern(Vector(Element(1), Element(2), Element(4)))
        ))

        val result = joiner.generateCandidates(source).collect()

        result should contain theSameElementsAs List(
          Pattern(Vector(Element(1), Element(2, 3), Element(4)))
        )
      }
    }

    "have internal `joinPatterns` method" which {
      "generates candidates by joining source patterns" in withPatternJoiner[Int] { joiner =>
        val source = sc.parallelize(sourcePatterns)

        val result = joiner.joinPatterns(source).collect()

        result should contain theSameElementsAs afterJoinPatterns
      }

      "correctly works for single-item patterns:" in withPatternJoiner[Int] { joiner =>
        val source = sc.parallelize(sourceSingleItemPatterns)

        val result = joiner.joinPatterns(source).collect()

        result should contain theSameElementsAs afterJoinSingleItemPatterns
      }
    }

    "have internal `pruneMatches` method" which {
      "filters out matches with subsequences not in source" in withPatternJoiner[Int] { joiner =>
        val source = sc.parallelize(sourcePatterns)
        val afterJoin = sc.parallelize(afterJoinPatterns)

        val result = joiner.pruneMatches(afterJoin, source).collect()

        result should contain theSameElementsAs afterPruningPatterns
      }

      "works correctly with matches generated from single-item patterns" in withPatternJoiner[Int] { joiner =>
        val source = sc.parallelize(sourceSingleItemPatterns)
        val afterJoin = sc.parallelize(afterJoinSingleItemPatterns)

        val result = joiner.pruneMatches(afterJoin, source).collect()

        result should contain theSameElementsAs afterPruneSingleItemPatterns
      }

      "works correctly with matches generated from double-item patterns" in withPatternJoiner[Int] { joiner =>
        val source = sc parallelize List(
          Pattern(Vector(Element(2), Element(4))),
          Pattern(Vector(Element(2), Element(3))),
          Pattern(Vector(Element(1), Element(2))),
          Pattern(Vector(Element(1), Element(4))),
          Pattern(Vector(Element(3), Element(4))),
          Pattern(Vector(Element(1), Element(3))))
        val afterJoin = sc parallelize List(
          Pattern(Vector(Element(1), Element(2), Element(3))),
          Pattern(Vector(Element(1), Element(2), Element(4))),
          Pattern(Vector(Element(2), Element(3), Element(4))),
          Pattern(Vector(Element(1), Element(3), Element(4))))

        val result = joiner.pruneMatches(afterJoin, source).collect()

        result should contain theSameElementsAs afterJoin.collect()
      }

      "works correctly for multi-item elements" in withPatternJoiner[Int] { joiner =>
        val source = sc parallelize List(
          Pattern(Vector(Element(1), Element(2), Element(3, 4))),
          Pattern(Vector(Element(1), Element(2), Element(3, 5))),
          Pattern(Vector(Element(1), Element(2), Element(4, 5))),
          Pattern(Vector(Element(1), Element(3, 4, 5))),
          Pattern(Vector(Element(2), Element(3, 4, 5))))
        val afterJoin = sc parallelize List(
          Pattern(Vector(Element(1), Element(2), Element(3, 4, 5))))

        val result = joiner.pruneMatches(afterJoin, source).collect()

        result should contain theSameElementsAs afterJoin.collect()
      }
    }
  }

  private val partitioner: Partitioner = new HashPartitioner(4)

  private def withPatternJoiner[ItemType: Ordering](f: PatternJoiner[ItemType] => Unit): Unit = {
    f(new PatternJoiner[ItemType](partitioner, None))
  }
}

class PatternSupportSpec extends WordSpec
  with Matchers {

  "PatternSupport" should {
    import PatternJoiner._

    val singleItemPattern: Pattern[Int] = Pattern(Vector(Element(1)))
    val singleElementMultipleItemsPattern: Pattern[Int] = Pattern(Vector(Element(1, 2)))
    val doubleElementsPattern: Pattern[Int] = Pattern(Vector(Element(1), Element(2)))
    val multipleItemElementsPattern: Pattern[Int] = Pattern(Vector(Element(1, 2, 3), Element(4, 5, 6)))

    "provide `prefixes` method" which {
      "returns correct result for single-item patterns" in {
        val prefixes = singleItemPattern.prefixes

        prefixes should contain theSameElementsAs List(
          PrefixResult(
            Pattern(Vector[Element[Int]]()),
            JoinItemNewElement(1)),
          PrefixResult(
            Pattern(Vector(Element[Int]())),
            JoinItemExistingElement(1)))
      }

      "returns correct result for single-element multiple-item patterns" in {
        val prefixes = singleElementMultipleItemsPattern.prefixes

        prefixes should contain theSameElementsAs List(
          PrefixResult(
            Pattern(Vector(Element(1))),
            JoinItemExistingElement(2)),
          PrefixResult(
            Pattern(Vector(Element(2))),
            JoinItemExistingElement(1)))
      }

      "returns correct result for longer patterns" in {
        val prefixes = doubleElementsPattern.prefixes

        prefixes should contain theSameElementsAs List(
          PrefixResult(
            Pattern(Vector(Element(1))),
            JoinItemNewElement(2)))
      }

      "returns correct result with multiple-items elements" in {
        val prefixes = multipleItemElementsPattern.prefixes

        prefixes should contain theSameElementsAs List(
          PrefixResult(
            Pattern(Vector(Element(1, 2, 3), Element(4, 5))),
            JoinItemExistingElement(6)),
          PrefixResult(
            Pattern(Vector(Element(1, 2, 3), Element(4, 6))),
            JoinItemExistingElement(5)),
          PrefixResult(
            Pattern(Vector(Element(1, 2, 3), Element(5, 6))),
            JoinItemExistingElement(4)))
      }
    }

    "provide `suffixes` method" which {
      "returns correct result for single-item patterns" in {
        val suffixes = singleItemPattern.suffixes

        suffixes should contain theSameElementsAs List(
          SuffixResult(
            Pattern(Vector[Element[Int]]()),
            JoinItemNewElement(1)),
          SuffixResult(
            Pattern(Vector(Element[Int]())),
            JoinItemExistingElement(1)))
      }

      "returns correct result for single-element multiple-item patterns" in {
        val suffixes = singleElementMultipleItemsPattern.suffixes

        suffixes should contain theSameElementsAs List(
          SuffixResult(
            Pattern(Vector(Element(1))),
            JoinItemExistingElement(2)),
          SuffixResult(
            Pattern(Vector(Element(2))),
            JoinItemExistingElement(1)))
      }

      "returns correct result for longer patterns" in {
        val suffixes = doubleElementsPattern.suffixes

        suffixes should contain theSameElementsAs List(
          SuffixResult(
            Pattern(Vector(Element(2))),
            JoinItemNewElement(1)))
      }

      "returns correct result with multiple-items elements" in {
        val suffixes = multipleItemElementsPattern.suffixes

        suffixes should contain theSameElementsAs List(
          SuffixResult(
            Pattern(Vector(Element(1, 2), Element(4, 5, 6))),
            JoinItemExistingElement(3)),
          SuffixResult(
            Pattern(Vector(Element(1, 3), Element(4, 5, 6))),
            JoinItemExistingElement(2)),
          SuffixResult(
            Pattern(Vector(Element(2, 3), Element(4, 5, 6))),
            JoinItemExistingElement(1)))
      }
    }
  }
}
