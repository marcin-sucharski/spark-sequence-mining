package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.PatternMatcher.SearchableSequence
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

private[gsp] class PatternMatcher[ItemType : ClassTag, TimeType : ClassTag, SequenceId : ClassTag](
  partitioner: Partitioner,
  sequences: RDD[(SequenceId, Transaction[ItemType, TimeType, SequenceId])]
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

}

private[gsp] object PatternMatcher {
  type OrderedItemOccurrenceList[TimeType] =
    IndexedSeq[TimeType]

  case class SearchableSequence[ItemType, TimeType, SequenceId](
    id: SequenceId,
    items: Map[ItemType, OrderedItemOccurrenceList[TimeType]]
  )
}