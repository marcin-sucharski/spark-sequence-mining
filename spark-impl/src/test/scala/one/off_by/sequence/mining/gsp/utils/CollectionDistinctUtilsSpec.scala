package one.off_by.sequence.mining.gsp.utils

import org.scalatest.{FreeSpec, Matchers}

import scala.util.Random

class CollectionDistinctUtilsSpec extends FreeSpec
  with Matchers {

  import CollectionDistinctUtils._

  "CollectionDistinctUtils should" - {
    "provide IteratorDistinct implicit class that" - {
      "has 'distinct' method which" - {
        "correctly works for many big examples" in {
          val count = 10
          val size = 10000

          for (_ <- 1 to count) {
            val collection = ((1 to size) ++ Random.shuffle(1 to size).take(size / 3)).toVector

            collection.iterator.distinct.toVector should contain theSameElementsAs collection.distinct
          }
        }
      }
    }
  }
}
