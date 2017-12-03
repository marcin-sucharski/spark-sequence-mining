package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.PatternMatcher.SearchableSequence
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkContext}

import scala.annotation.tailrec
import scala.collection.Searching
import scala.collection.immutable.HashMap
import scala.language.postfixOps
import scala.reflect.ClassTag

private[gsp] class PatternMatcher[ItemType: Ordering, TimeType, DurationType, SequenceId: ClassTag](
  sc: SparkContext,
  partitioner: Partitioner,
  sequences: RDD[(SequenceId, Transaction[ItemType, TimeType, SequenceId])],
  gspOptions: Option[GSPOptions[TimeType, DurationType]],
  minSupportCount: Long
)(implicit timeOrdering: Ordering[TimeType]) extends LoggingUtils {
  type TransactionType = Transaction[ItemType, TimeType, SequenceId]
  type SearchableSequenceType = SearchableSequence[ItemType, TimeType, SequenceId]
  type SupportCount = Long

  private[gsp] val searchableSequences: RDD[(Iterable[TransactionType], SearchableSequenceType)] = {
    implicit val localOrdering: Ordering[TimeType] = timeOrdering
    sequences.groupByKey()
      .values
      .map { sequence =>
        (sequence, PatternMatcher.buildSearchableSequence(sequence)(localOrdering))
      }
      .repartition(partitioner.numPartitions)
      .cache()
  }

  def filter(in: RDD[Pattern[ItemType]]): RDD[(Pattern[ItemType], SupportCount)] = {
    val itemOrderingLocal = implicitly[Ordering[ItemType]]
    val timeOrderingLocal = timeOrdering
    val minSupportCountLocal = minSupportCount
    val gspOptionsLocal = gspOptions

    val hashTrees = in mapPartitions { patterns =>
      val empty = HashTree.empty[ItemType, TimeType, DurationType, SequenceId](itemOrderingLocal, timeOrderingLocal)
      ((empty /: patterns) (_ add _) :: Nil).toIterator
    } cache()

    logger trace {
      val beforeMatching = hashTrees cartesian searchableSequences flatMap { case (hashTree, (sequence, _)) =>
        hashTree.findPossiblePatterns(gspOptionsLocal, sequence)
      } map (_.toString)
      s"Candidates before matching: ${beforeMatching.distinct().toPrettyList}"
    }

    val counted = searchableSequences cartesian hashTrees flatMap { case ((sequence, searchable), hashTree) =>
      hashTree.findPossiblePatterns(gspOptionsLocal, sequence) filter { pattern =>
        PatternMatcher.matches(pattern, searchable, gspOptionsLocal)(timeOrderingLocal)
      } map (p => (p, 1L))
    } reduceByKey (_ + _)

    logger trace {
      val humanReadable = counted.map(p => s"${p._1} -> ${p._2}")
      s"Counted candidates in pattern matcher: ${humanReadable.toPrettyList}"
    }

    counted filter { case (_, support) =>
      support >= minSupportCountLocal
    }
  }
}

private[gsp] object PatternMatcher {
  type OrderedItemOccurrenceList[TimeType] =
    IndexedSeq[TimeType]

  case class SearchableSequence[ItemType, TimeType, SequenceId](
    id: SequenceId,
    items: Map[ItemType, OrderedItemOccurrenceList[TimeType]],

    // first and last element times are passed as arguments because they require time
    // ordering for calculation (and SearchableSequence may be serialized later so it
    // should be as small as possible)
    firstElementTime: TimeType,
    lastElementTime: TimeType
  ) {
    assume(items.forall(_._2.nonEmpty))
    assume(items.nonEmpty)

    def findFirstOccurrence(item: ItemType): Option[TimeType] =
      items.get(item).flatMap(_.headOption)

    def findFirstOccurrenceSameOrAfter(
      time: TimeType,
      item: ItemType
    )(implicit timeOrdering: Ordering[TimeType]): Option[TimeType] =
      items.get(item) flatMap { orderedItemsOfType =>
        import Searching._
        orderedItemsOfType.search(time) match {
          case Found(_)              => Some(time)
          case InsertionPoint(index) =>
            if (index >= orderedItemsOfType.size) None
            else Some(orderedItemsOfType(index))
        }
      }

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

  def buildSearchableSequence[ItemType, TimeType, SequenceId](
    sequence: Iterable[Transaction[ItemType, TimeType, SequenceId]]
  )(implicit timeOrdering: Ordering[TimeType]): SearchableSequence[ItemType, TimeType, SequenceId] = {
    assume(sequence.nonEmpty)
    assume(sequence.forall(_.items.nonEmpty))

    val groupedNotSorted = (HashMap[ItemType, Vector[TimeType]]() /: sequence) { case (hashMap, transaction) =>
      (hashMap /: transaction.items) { case (hashMapLocal, item) =>
        val occurrences = hashMapLocal.getOrElse(item, Vector[TimeType]())
        hashMapLocal.updated(item, occurrences :+ transaction.time)
      }
    }
    val items = groupedNotSorted
      .mapValues(_.sorted.toIndexedSeq)
      .map(identity) // https://issues.scala-lang.org/browse/SI-7005

    SearchableSequence[ItemType, TimeType, SequenceId](
      sequence.head.sequenceId,
      items,
      items.values.minBy(_.head).head,
      items.values.maxBy(_.last).last
    )
  }

  case class MinTime[TimeType](
    time: TimeType,
    inclusive: Boolean
  )

  def matches[ItemType, TimeType, DurationType, SequenceId](
    pattern: Pattern[ItemType],
    seq: SearchableSequence[ItemType, TimeType, SequenceId],
    gspOptions: Option[GSPOptions[TimeType, DurationType]]
  )(implicit timeOrdering: Ordering[TimeType]): Boolean = {
    assume(seq.items.forall(_._2.nonEmpty))
    assume(seq.items.nonEmpty)

    trait PatternExtendResult
    case object PatternExtendSuccess extends PatternExtendResult
    case class PatternExtendBacktrace(minTime: MinTime[TimeType]) extends PatternExtendResult
    case object PatternExtendFailure extends PatternExtendResult

    val elementFinder: ElementFinder[ItemType, TimeType] = gspOptions collect {
      case GSPOptions(typeSupport, Some(windowSize), _, _) =>
        import typeSupport.durationOrdering
        new SlidingWindowElementFinder[ItemType, TimeType, SequenceId, DurationType](seq, windowSize, typeSupport)
    } getOrElse new SimpleElementFinder[ItemType, TimeType, SequenceId](seq)

    val applyMinGap: TimeType => MinTime[TimeType] = gspOptions collect {
      case GSPOptions(typeSupport, _, Some(minGap), _) =>
        (endTime: TimeType) => MinTime(typeSupport.timeAdd(endTime, minGap), inclusive = true)
    } getOrElse ((endTime: TimeType) => MinTime(endTime, inclusive = false))

    // returns max start time for next element
    val applyMaxGap: TimeType => TimeType = gspOptions collect {
      case GSPOptions(typeSupport, _, _, Some(maxGap)) =>
        typeSupport.timeAdd(_: TimeType, maxGap)
    } getOrElse (_ => seq.lastElementTime)

    // returns minimum time of previous element
    val applyMaxGapBack: TimeType => TimeType = gspOptions collect {
      case GSPOptions(typeSupport, _, _, Some(maxGap)) =>
        typeSupport.timeSubtract(_: TimeType, maxGap)
    } getOrElse (_ => seq.firstElementTime)

    def extend(minTime: MinTime[TimeType], maxTime: TimeType, rest: Pattern[ItemType]): PatternExtendResult =
      if (rest.elements.isEmpty) PatternExtendSuccess
      else {
        val element = rest.elements.head
        elementFinder.find(minTime, element) match {
          case Some((startTime, _)) if timeOrdering.gt(startTime, maxTime) =>
            PatternExtendBacktrace(MinTime(applyMaxGapBack(startTime), inclusive = true))

          case Some((_, endTime)) =>
            extend(applyMinGap(endTime), applyMaxGap(endTime), Pattern(rest.elements.tail)) match {
              case PatternExtendBacktrace(newMinTime) => extend(newMinTime, maxTime, rest)
              case x                                  => x
            }

          case None =>
            PatternExtendFailure
        }
      }

    extend(MinTime(seq.firstElementTime, inclusive = true), seq.lastElementTime, pattern) match {
      case PatternExtendSuccess => true
      case _                    => false
    }
  }

  trait ElementFinder[ItemType, TimeType] {
    type TransactionStartTime = TimeType
    type TransactionEndTime = TimeType

    def find(minTime: MinTime[TimeType], element: Element[ItemType]): Option[(TransactionStartTime, TransactionEndTime)]
  }

  trait FindFirstHelper[ItemType, TimeType, SequenceId] {
    def sequence: SearchableSequence[ItemType, TimeType, SequenceId]

    implicit def timeOrdering: Ordering[TimeType]

    protected def findFirst(minTime: MinTime[TimeType], item: ItemType): Option[TimeType] =
      if (minTime.inclusive) sequence.findFirstOccurrenceSameOrAfter(minTime.time, item)
      else sequence.findFirstOccurrenceAfter(minTime.time, item)
  }

  class SimpleElementFinder[ItemType, TimeType, SequenceId](
    val sequence: SearchableSequence[ItemType, TimeType, SequenceId]
  )(implicit val timeOrdering: Ordering[TimeType]) extends ElementFinder[ItemType, TimeType]
    with FindFirstHelper[ItemType, TimeType, SequenceId] {

    @tailrec
    final override def find(
      minTime: MinTime[TimeType],
      element: Element[ItemType]
    ): Option[(TransactionStartTime, TransactionEndTime)] =
      findFirst(minTime, element.items.head) match {
        case Some(time) =>
          val times = element.items.tail.map(findFirst(minTime, _))
          if (times.forall(_.isDefined)) {
            val sameTransaction = times.forall(_.contains(time))
            if (sameTransaction) Some((time, time))
            else {
              val newMinTime = MinTime(timeOrdering.max(times.view.flatten.max, time), inclusive = true)
              find(newMinTime, element)
            }
          } else None

        case None => None
      }
  }

  class SlidingWindowElementFinder[
  ItemType, TimeType, SequenceId, DurationType
  ](
    val sequence: SearchableSequence[ItemType, TimeType, SequenceId],
    windowSize: DurationType,
    typeSupport: GSPTypeSupport[TimeType, DurationType]
  )(implicit
    val timeOrdering: Ordering[TimeType],
    durationOrdering: Ordering[DurationType]
  ) extends ElementFinder[ItemType, TimeType]
    with FindFirstHelper[ItemType, TimeType, SequenceId] {

    private implicit val implicitTypeSupport: GSPTypeSupport[TimeType, DurationType] = typeSupport

    import Helper.TimeSupport

    @tailrec
    final override def find(
      minTime: MinTime[TimeType],
      element: Element[ItemType]
    ): Option[(TransactionStartTime, TransactionEndTime)] = {
      val maybeTimes = element.items.map(findFirst(minTime, _))
      if (maybeTimes.forall(_.isDefined)) {
        val first = maybeTimes.view.flatten.min
        val last = maybeTimes.view.flatten.max
        val duration = first distanceTo last

        if (durationOrdering.lteq(duration, windowSize)) Some((first, last))
        else find(MinTime(last - windowSize, inclusive = true), element)
      } else None
    }
  }

}
