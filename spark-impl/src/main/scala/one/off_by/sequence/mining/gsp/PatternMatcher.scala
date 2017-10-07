package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.PatternMatcher.SearchableSequence
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec
import scala.collection.Searching
import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

private[gsp] class PatternMatcher[ItemType, TimeType, DurationType, SequenceId: ClassTag](
  partitioner: Partitioner,
  sequences: RDD[(SequenceId, Transaction[ItemType, TimeType, SequenceId])],
  gspOptions: Option[GSPOptions[TimeType, DurationType]]
)(implicit timeOrdering: Ordering[TimeType]) {
  type TransactionType = Transaction[ItemType, TimeType, SequenceId]
  type SearchableSequenceType = SearchableSequence[ItemType, TimeType, SequenceId]

  private[gsp] val searchableSequences: RDD[(SequenceId, SearchableSequenceType)] = {
    implicit val localOrdering: Ordering[TimeType] = timeOrdering
    sequences.groupByKey()
      .mapValues { sequence =>
        val groupedNotSorted = (HashMap[ItemType, Vector[TimeType]]() /: sequence) { case (hashMap, transaction) =>
          (hashMap /: transaction.items) { case (hashMapLocal, item) =>
            val occurrences = hashMapLocal.getOrElse(item, Vector[TimeType]())
            hashMapLocal.updated(item, occurrences :+ transaction.time)
          }
        }
        SearchableSequence[ItemType, TimeType, SequenceId](
          sequence.head.sequenceId,
          groupedNotSorted
            .mapValues(_.sorted(localOrdering).toIndexedSeq)
            .map(identity) // https://issues.scala-lang.org/browse/SI-7005
        )
      }
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  private[gsp] def matches(pattern: Pattern[ItemType], seq: SearchableSequenceType): Boolean = {
    case class State(

    )




    ???
  }
}

private[gsp] object PatternMatcher {
  type OrderedItemOccurrenceList[TimeType] =
    IndexedSeq[TimeType]

  case class SearchableSequence[ItemType, TimeType, SequenceId](
    id: SequenceId,
    items: Map[ItemType, OrderedItemOccurrenceList[TimeType]]
  ) {
    def findFirstOccurrence(item: ItemType): Option[TimeType] =
      items.get(item).flatMap(_.headOption)

    def findFirstOccurrenceAfter(
      time: TimeType,
      item: ItemType
    )(implicit timeOrdering: Ordering[TimeType]): Option[TimeType] =
      items.get(item) flatMap { orderedItemsOfType =>
        findFirstAfter(orderedItemsOfType, time)
      }

    @tailrec
    private def findFirstAfter(
      sequence: OrderedItemOccurrenceList[TimeType],
      time: TimeType
    )(implicit timeOrdering: Ordering[TimeType]): Option[TimeType] = {
      assert(sequence.nonEmpty)
      val idx = (sequence.size - 1) / 2
      sequence(idx) match {
        case value if timeOrdering.equiv(value, time) =>
          sequence.lift(idx + 1)

        case value if timeOrdering.gt(value, time) =>
          if (idx == 0) Some(sequence(0))
          else findFirstAfter(sequence.take(idx + 1), time)

        case value if timeOrdering.lt(value, time) =>
          if (sequence.size == 1) None
          else findFirstAfter(sequence.drop(idx + 1), time)
      }
    }
  }

}
