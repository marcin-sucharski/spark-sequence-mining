package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.PatternMatcher.SearchableSequence
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

@specialized
private[gsp] class PatternMatcher[ItemType, TimeType, DurationType, SequenceId: ClassTag](
  partitioner: Partitioner,
  sequences: RDD[(SequenceId, Transaction[ItemType, TimeType, SequenceId])],
  zeroTime: TimeType,
  gspOptions: Option[GSPOptions[TimeType, DurationType]]
)(implicit timeOrdering: Ordering[TimeType]) {
  type TransactionType = Transaction[ItemType, TimeType, SequenceId]
  type SearchableSequenceType = SearchableSequence[ItemType, TimeType, SequenceId]

  private[gsp] val searchableSequences: RDD[(SequenceId, SearchableSequenceType)] = {
    implicit val localOrdering: Ordering[TimeType] = timeOrdering
    sequences.groupByKey()
      .mapValues { sequence =>
        PatternMatcher.buildSearchableSequence(sequence)(localOrdering)
      }
      .persist(StorageLevel.MEMORY_AND_DISK)
  }
}

private[gsp] object PatternMatcher {
  type OrderedItemOccurrenceList[TimeType] =
    IndexedSeq[TimeType]

  @specialized
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

  @specialized
  def buildSearchableSequence[ItemType, TimeType, SequenceId](
    sequence: Iterable[Transaction[ItemType, TimeType, SequenceId]]
  )(implicit timeOrdering: Ordering[TimeType]): SearchableSequence[ItemType, TimeType, SequenceId] = {
    val groupedNotSorted = (HashMap[ItemType, Vector[TimeType]]() /: sequence) { case (hashMap, transaction) =>
      (hashMap /: transaction.items) { case (hashMapLocal, item) =>
        val occurrences = hashMapLocal.getOrElse(item, Vector[TimeType]())
        hashMapLocal.updated(item, occurrences :+ transaction.time)
      }
    }
    SearchableSequence[ItemType, TimeType, SequenceId](
      sequence.head.sequenceId,
      groupedNotSorted
        .mapValues(_.sorted.toIndexedSeq)
        .map(identity) // https://issues.scala-lang.org/browse/SI-7005
    )
  }

  @specialized
  def matches[ItemType, TimeType, DurationType, SequenceId](
    pattern: Pattern[ItemType],
    seq: SearchableSequence[ItemType, TimeType, SequenceId],
    zeroTime: TimeType,
    gspOptions: Option[GSPOptions[TimeType, DurationType]]
  ): Boolean = {
    trait PatternExtendResult
    case object PatternExtendSuccess extends PatternExtendResult
    case class PatternExtendBacktrace(minTime: TimeType) extends PatternExtendResult
    case object PatternExtendFailure extends PatternExtendResult

    def extend(timePosition: TimeType, rest: Pattern[ItemType]): PatternExtendResult = {
      ???
    }

    extend(zeroTime, pattern) match {
      case PatternExtendSuccess => true
      case _                    => false
    }
  }

  @specialized
  trait ElementFinder[ItemType, TimeType] {
    type TransactionStartTime = TimeType
    type TransactionEndTime = TimeType

    def find(minTime: TimeType, element: Element[ItemType]): Option[(TransactionStartTime, TransactionEndTime)]
  }

  @specialized
  class SimpleElementFinder[ItemType, TimeType, SequenceId](
    sequence: SearchableSequence[ItemType, TimeType, SequenceId]
  )(implicit timeOrdering: Ordering[TimeType]) extends ElementFinder[ItemType, TimeType] {
    @tailrec
    final override def find(
      minTime: TimeType,
      element: Element[ItemType]
    ): Option[(TransactionStartTime, TransactionEndTime)] =
      sequence.findFirstOccurrenceAfter(minTime, element.items.head) match {
        case Some(time) =>
          val times = element.items.tail.map(sequence.findFirstOccurrenceAfter(minTime, _))
          if (times.forall(_.isDefined)) {
            val sameTransaction = times.forall(_.contains(time))
            if (sameTransaction) Some((time, time)) else find(time, element)
          } else None

        case None => None
      }
  }

  @specialized
  class SlidingWindowElementFinder[ItemType, TimeType, SequenceId, DurationType](
    sequence: SearchableSequence[ItemType, TimeType, SequenceId],
    windowSize: DurationType,
    typeSupport: GSPTypeSupport[TimeType, DurationType]
  )(implicit
    timeOrdering: Ordering[TimeType],
    durationOrdering: Ordering[DurationType]
  ) extends ElementFinder[ItemType, TimeType] {

    private implicit val implicitTypeSupport: GSPTypeSupport[TimeType, DurationType] = typeSupport
    import Helper.TimeSupport

    @tailrec
    final override def find(
      minTime: TimeType,
      element: Element[ItemType]
    ): Option[(TransactionStartTime, TransactionEndTime)] = {
      val maybeTimes = element.items.map(sequence.findFirstOccurrenceAfter(minTime, _))
      if (maybeTimes.forall(_.isDefined)) {
        val first = maybeTimes.view.flatten.min
        val last = maybeTimes.view.flatten.max
        val duration = first distanceTo last

        if (durationOrdering.lteq(duration, windowSize)) Some((first, last))
        else find(last - windowSize, element)
      } else None
    }
  }
}
