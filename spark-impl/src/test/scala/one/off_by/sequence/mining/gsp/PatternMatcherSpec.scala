package one.off_by.sequence.mining.gsp

import one.off_by.testkit.SparkTestBase
import org.apache.spark.{HashPartitioner, Partitioner}
import org.scalatest.{Inspectors, Matchers, WordSpec}

class PatternMatcherSpec extends WordSpec
  with Matchers
  with Inspectors
  with SparkTestBase {

  "PatternMatcher" should {
    "have internal `searchableSequences` field" which {
      "contains correct helper data for searching" in {
        val input = List[Transaction[Int, Int, Int]](
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
        ).map(x => (x.id, x))

        val sequences = sc.parallelize(input)
        val patternMatcher = new PatternMatcher[Int, Int, Int](partitioner, sequences)

        patternMatcher.searchableSequences.collect() should contain theSameElementsAs output
      }
    }
  }

  private val partitioner: Partitioner = new HashPartitioner(4)
}
