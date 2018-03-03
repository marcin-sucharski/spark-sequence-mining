package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.PatternMatcher.SearchableSequence
import one.off_by.sequence.mining.gsp.utils.{DummySpecializedValue, DummySpecializer, LoggingUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{Partitioner, SparkContext}

import scala.annotation.tailrec
import scala.collection.{Searching, mutable}
import scala.collection.immutable.HashMap
import scala.language.postfixOps
import scala.reflect.ClassTag

private[gsp] class PatternMatcher[
@specialized(Int, Long) ItemType: Ordering,
@specialized(Int, Long) TimeType: ClassTag,
@specialized(Int, Long) DurationType,
SequenceId: ClassTag
](
  sc: SparkContext,
  partitioner: Partitioner,
  sequences: RDD[(SequenceId, Transaction[ItemType, TimeType, SequenceId])],
  gspOptions: Option[GSPOptions[TimeType, DurationType]],
  minSupportCount: Int
)(implicit timeOrdering: Ordering[TimeType]) extends LoggingUtils {
  type TransactionType = Transaction[ItemType, TimeType, SequenceId]
  type SearchableSequenceType = SearchableSequence[ItemType, TimeType, SequenceId]
  type SupportCount = Int

  private[gsp] val searchableSequences: Broadcast[Array[(Vector[TransactionType], SearchableSequenceType)]] = {
    val localClassTag: ClassTag[TimeType] = implicitly[ClassTag[TimeType]]
    val localOrdering: Ordering[TimeType] = timeOrdering
    sc.broadcast {
      sequences.groupByKey()
        .values
        .map(_.toVector)
        .map { sequence =>
          (sequence, PatternMatcher.buildSearchableSequence(sequence)(
            localClassTag,
            localOrdering
          ))
        }
        .collect()
    }
  }

  private def makeEmptyHashTree(): HashTree[ItemType, TimeType, DurationType, SequenceId] =
    HashTreeLeaf[ItemType, TimeType, DurationType, SequenceId]()(implicitly[Ordering[ItemType]], timeOrdering)

  def filter(
    in: RDD[Pattern[ItemType]],
    maybeCandidateCountAccumulator: Option[LongAccumulator] = None,
    dummyItemType: DummySpecializer[ItemType] = DummySpecializedValue[ItemType](),
    dummyTimeType: DummySpecializer[TimeType] = DummySpecializedValue[TimeType](),
    dummyDurationType: DummySpecializer[DurationType] = DummySpecializedValue[DurationType]()
  ): RDD[(Pattern[ItemType], SupportCount)] = {
    val searchableSequencesBV = searchableSequences
    val timeOrderingLocal = timeOrdering
    val minSupportCountLocal = minSupportCount
    val gspOptionsLocal = gspOptions

    val emptyHashTree = makeEmptyHashTree()
    val hashTrees = in mapPartitions { patterns =>
      val resultTree = patterns.foldLeft(emptyHashTree) { case (tree, pattern) =>
        maybeCandidateCountAccumulator.foreach(_.add(1L))
        tree.add(pattern)
      }
      (resultTree :: Nil).toIterator
    }

    val counted = hashTrees flatMap { hashTree =>
      searchableSequencesBV.value flatMap { case (sequence, searchable) =>
        hashTree.findPossiblePatterns(gspOptionsLocal, sequence) filter { pattern =>
          PatternMatcher.matches(pattern, searchable, gspOptionsLocal)(timeOrderingLocal)
        }
      } map (p => (p, 1))
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

object PatternMatcher {
  type OrderedItemOccurrenceList[TimeType] = IndexedSeq[TimeType]

  case class SearchableSequence[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType,
  SequenceId
  ](
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
            if (index >= orderedItemsOfType.length) None
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
      val idx = (sequence.length - 1) / 2
      sequence(idx) match {
        case value if timeOrdering.equiv(value, time) =>
          sequence.lift(idx + 1)

        case value if timeOrdering.gt(value, time) =>
          if (idx == 0) Some(sequence(0))
          else findFirstAfter(sequence.take(idx + 1), time)

        case value if timeOrdering.lt(value, time) =>
          if (sequence.length == 1) None
          else findFirstAfter(sequence.drop(idx + 1), time)
      }
    }
  }

  def buildSearchableSequence[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType: ClassTag,
  SequenceId
  ](
    sequence: Vector[Transaction[ItemType, TimeType, SequenceId]]
  )(implicit timeOrdering: Ordering[TimeType]): SearchableSequence[ItemType, TimeType, SequenceId] = {
    assume(sequence.nonEmpty)
    assume(sequence.forall(_.items.nonEmpty))

    val groupedNotSorted = (HashMap[ItemType, Vector[TimeType]]() /: sequence) { case (hashMap, transaction) =>
      val transactionTime: TimeType = transaction.time
      (hashMap /: transaction.items) { case (hashMapLocal, item) =>
        val occurrences = hashMapLocal.getOrElse(item, Vector[TimeType]())
        hashMapLocal.updated(item, occurrences :+ transactionTime)
      }
    }
    val items = groupedNotSorted
      .mapValues { occurences =>
        val array = new Array[TimeType](occurences.size)
        occurences.sorted.copyToArray(array)
        mutable.WrappedArray.make[TimeType](array)
      }
      .map(identity) // https://issues.scala-lang.org/browse/SI-7005

    SearchableSequence[ItemType, TimeType, SequenceId](
      sequence.head.sequenceId,
      items,
      items.values.minBy(_.head).head,
      items.values.maxBy(_.last).last
    )
  }

  def matches[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType,
  @specialized(Int, Long) DurationType,
  SequenceId
  ](
    pattern: Pattern[ItemType],
    seq: SearchableSequence[ItemType, TimeType, SequenceId],
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    dummyItemType: DummySpecializer[ItemType] = DummySpecializedValue[ItemType](),
    dummyTimeType: DummySpecializer[TimeType] = DummySpecializedValue[TimeType](),
    dummyDurationType: DummySpecializer[DurationType] = DummySpecializedValue[DurationType]()
  )(implicit timeOrdering: Ordering[TimeType]): Boolean = {
    assume(seq.items.forall(_._2.nonEmpty))
    assume(seq.items.nonEmpty)

    val elementFinder: ElementFinder[ItemType, TimeType, DurationType] =
      createElementFinderFromOptions[ItemType, TimeType, DurationType, SequenceId](gspOptions, seq)(timeOrdering)

    val applyMinGap: TimeType => MinTime[TimeType] =
      createApplyMinGap[TimeType, SequenceId, DurationType](gspOptions)

    // returns maximum start time for next element
    val applyMaxGap: TimeType => TimeType =
      createApplyMaxGap[ItemType, TimeType, SequenceId, DurationType](gspOptions, seq)

    // returns minimum time of previous element
    val applyMaxGapBack: TimeType => TimeType =
      createApplyMaxGapBack[ItemType, TimeType, SequenceId, DurationType](gspOptions, seq)

    def extend(minTime: MinTime[TimeType], maxTime: TimeType, rest: Pattern[ItemType]): PatternExtendResult =
      if (rest.elements.isEmpty) PatternExtendSuccess
      else {
        val element = rest.elements.head
        elementFinder.find(minTime, element) match {
          case Some((startTime, _)) if timeOrdering.gt(startTime, maxTime) =>
            PatternExtendBacktrace(MinTime(applyMaxGapBack(startTime), inclusive = true))

          case Some((_, endTime)) =>
            extend(applyMinGap(endTime), applyMaxGap(endTime), Pattern(rest.elements.tail)) match {
              case PatternExtendBacktrace(newMinTime: MinTime[TimeType]) => extend(newMinTime, maxTime, rest)
              case x                                                     => x
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

  private[this] sealed trait PatternExtendResult
  private[this] final case object PatternExtendSuccess extends PatternExtendResult
  private[this] final case class PatternExtendBacktrace[@specialized(Int, Long) TimeType](
    minTime: MinTime[TimeType]
  ) extends PatternExtendResult
  private[this] final case object PatternExtendFailure extends PatternExtendResult

  final case class MinTime[@specialized(Int, Long) TimeType](
    time: TimeType,
    inclusive: Boolean
  )

  sealed trait ElementFinder[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType,
  @specialized(Int, Long) DurationType
  ] {
    type TransactionStartTime = TimeType
    type TransactionEndTime = TimeType

    def find(
      minTime: MinTime[TimeType],
      element: Element[ItemType],
      dummyItemType: DummySpecializer[ItemType] = DummySpecializedValue[ItemType](),
      dummyTimeType: DummySpecializer[TimeType] = DummySpecializedValue[TimeType](),
      dummyDurationType: DummySpecializer[DurationType] = DummySpecializedValue[DurationType]()
    ): Option[(TransactionStartTime, TransactionEndTime)]
  }

  trait FindFirstHelper[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType,
  SequenceId
  ] {
    def sequence: SearchableSequence[ItemType, TimeType, SequenceId]

    implicit def timeOrdering: Ordering[TimeType]

    protected final def findFirst(minTime: MinTime[TimeType], item: ItemType): Option[TimeType] =
      if (minTime.inclusive) sequence.findFirstOccurrenceSameOrAfter(minTime.time, item)
      else sequence.findFirstOccurrenceAfter(minTime.time, item)
  }

  final class SimpleElementFinder[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType,
  @specialized(Int, Long) DurationType,
  SequenceId
  ](
    val sequence: SearchableSequence[ItemType, TimeType, SequenceId]
  )(implicit val timeOrdering: Ordering[TimeType]) extends ElementFinder[ItemType, TimeType, DurationType]
    with FindFirstHelper[ItemType, TimeType, SequenceId] {

    @tailrec
    override def find(
      minTime: MinTime[TimeType],
      element: Element[ItemType],
      dummyItemType: DummySpecializer[ItemType] = DummySpecializedValue[ItemType](),
      dummyTimeType: DummySpecializer[TimeType] = DummySpecializedValue[TimeType](),
      dummyDurationType: DummySpecializer[DurationType] = DummySpecializedValue[DurationType]()
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

  final class SlidingWindowElementFinder[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType,
  SequenceId,
  @specialized(Int, Long) DurationType
  ](
    val sequence: SearchableSequence[ItemType, TimeType, SequenceId],
    windowSize: DurationType,
    typeSupport: GSPTypeSupport[TimeType, DurationType]
  )(implicit
    val timeOrdering: Ordering[TimeType],
    durationOrdering: Ordering[DurationType]
  ) extends ElementFinder[ItemType, TimeType, DurationType]
    with FindFirstHelper[ItemType, TimeType, SequenceId] {

    private implicit val implicitTypeSupport: GSPTypeSupport[TimeType, DurationType] = typeSupport

    import Helper.TimeSupport

    @tailrec
    override def find(
      minTime: MinTime[TimeType],
      element: Element[ItemType],
      dummyItemType: DummySpecializer[ItemType] = DummySpecializedValue[ItemType](),
      dummyTimeType: DummySpecializer[TimeType] = DummySpecializedValue[TimeType](),
      dummyDurationType: DummySpecializer[DurationType] = DummySpecializedValue[DurationType]()
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

  def createElementFinderFromOptions[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType,
  @specialized(Int, Long) DurationType,
  SequenceId
  ](
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    seq: SearchableSequence[ItemType, TimeType, SequenceId],
    dummyItemType: DummySpecializer[ItemType] = DummySpecializedValue[ItemType](),
    dummyTimeType: DummySpecializer[TimeType] = DummySpecializedValue[TimeType](),
    dummyDurationType: DummySpecializer[DurationType] = DummySpecializedValue[DurationType]()
  )(implicit timeOrdering: Ordering[TimeType]): ElementFinder[ItemType, TimeType, DurationType] =
    gspOptions match {
      case Some(GSPOptions(typeSupport, Some(windowSize), _, _)) =>
        import typeSupport.durationOrdering
        createSlidingWindowElementFinder[ItemType, TimeType, SequenceId, DurationType](seq, windowSize, typeSupport)

      case _ =>
        createSimpleElementFinder[ItemType, TimeType, DurationType, SequenceId](seq)
    }

  def createApplyMinGap[
  @specialized(Int, Long) TimeType,
  SequenceId,
  @specialized(Int, Long) DurationType](
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    dummyTimeType: DummySpecializer[TimeType] = DummySpecializedValue[TimeType](),
    dummyDurationType: DummySpecializer[DurationType] = DummySpecializedValue[DurationType]()
  ): TimeType => MinTime[TimeType] =
    gspOptions match {
      case Some(GSPOptions(typeSupport, _, Some(minGap), _)) =>
        (endTime: TimeType) => MinTime(typeSupport.timeAdd(endTime, minGap), inclusive = true)

      case _ =>
        (endTime: TimeType) => MinTime(endTime, inclusive = false)
    }

  def createApplyMaxGap[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType,
  SequenceId,
  @specialized(Int, Long) DurationType
  ](
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    seq: SearchableSequence[ItemType, TimeType, SequenceId],
    dummyItemType: DummySpecializer[ItemType] = DummySpecializedValue[ItemType](),
    dummyTimeType: DummySpecializer[TimeType] = DummySpecializedValue[TimeType](),
    dummyDurationType: DummySpecializer[DurationType] = DummySpecializedValue[DurationType]()
  ): TimeType => TimeType =
    gspOptions match {
      case Some(GSPOptions(typeSupport, _, _, Some(maxGap))) =>
        typeSupport.timeAdd(_: TimeType, maxGap)

      case _ =>
        (_: TimeType) => seq.lastElementTime
    }

  def createApplyMaxGapBack[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType,
  SequenceId,
  @specialized(Int, Long) DurationType
  ](
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    seq: SearchableSequence[ItemType, TimeType, SequenceId],
    dummyItemType: DummySpecializer[ItemType] = DummySpecializedValue[ItemType](),
    dummyTimeType: DummySpecializer[TimeType] = DummySpecializedValue[TimeType](),
    dummyDurationType: DummySpecializer[DurationType] = DummySpecializedValue[DurationType]()
  ): TimeType => TimeType =
    gspOptions match {
      case Some(GSPOptions(typeSupport, _, _, Some(maxGap))) =>
        typeSupport.timeSubtract(_: TimeType, maxGap)

      case _ =>
        (_: TimeType) => seq.firstElementTime
    }

  def createSlidingWindowElementFinder[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType,
  SequenceId,
  @specialized(Int, Long) DurationType
  ](
    sequence: SearchableSequence[ItemType, TimeType, SequenceId],
    windowSize: DurationType,
    typeSupport: GSPTypeSupport[TimeType, DurationType],
    dummyItemType: DummySpecializer[ItemType] = DummySpecializedValue[ItemType](),
    dummyTimeType: DummySpecializer[TimeType] = DummySpecializedValue[TimeType](),
    dummyDurationType: DummySpecializer[DurationType] = DummySpecializedValue[DurationType]()
  )(implicit
    timeOrdering: Ordering[TimeType],
    durationOrdering: Ordering[DurationType]
  ): SlidingWindowElementFinder[ItemType, TimeType, SequenceId, DurationType] =
    new SlidingWindowElementFinder[ItemType, TimeType, SequenceId, DurationType](sequence, windowSize, typeSupport)

  def createSimpleElementFinder[
  @specialized(Int, Long) ItemType,
  @specialized(Int, Long) TimeType,
  @specialized(Int, Long) DurationType,
  SequenceId
  ](
    sequence: SearchableSequence[ItemType, TimeType, SequenceId],
    dummyItemType: DummySpecializer[ItemType] = DummySpecializedValue[ItemType](),
    dummyTimeType: DummySpecializer[TimeType] = DummySpecializedValue[TimeType](),
    dummyDurationType: DummySpecializer[DurationType] = DummySpecializedValue[DurationType]()
  )(implicit
    timeOrdering: Ordering[TimeType]
  ): SimpleElementFinder[ItemType, TimeType, DurationType, SequenceId] =
    new SimpleElementFinder[ItemType, TimeType, DurationType, SequenceId](sequence)
}
