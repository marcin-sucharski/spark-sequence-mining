package one.off_by.sequence.mining.gsp

import scala.collection.immutable.HashMap
import scala.language.postfixOps

private[gsp] sealed trait HashTree[
ItemType,
@specialized(Int, Long) TimeType,
@specialized(Int, Long) DurationType,
SequenceId] {
  type SelfType = HashTree[ItemType, TimeType, DurationType, SequenceId]
  type TransactionType = Transaction[ItemType, TimeType, SequenceId]
  type PatternType = Pattern[ItemType]

  def add(pattern: PatternType): SelfType

  def findPossiblePatterns(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: Iterable[TransactionType]): Iterator[PatternType]

  protected[gsp] def addInternal(
    pattern: PatternType,
    iterable: Iterable[ItemType]
  ): SelfType

  protected[gsp] def findPossiblePatternsInternal(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: List[(TimeType, ItemType)]): Iterator[PatternType]
}

private[gsp] final case class HashTreeNode[
ItemType: Ordering,
@specialized(Int, Long) TimeType: Ordering,
@specialized(Int, Long) DurationType,
SequenceId](
  depth: Int,
  children: Map[Int, HashTree[ItemType, TimeType, DurationType, SequenceId]]
) extends HashTree[ItemType, TimeType, DurationType, SequenceId] {

  import HashTreeUtils._

  override def add(pattern: PatternType): HashTree[ItemType, TimeType, DurationType, SequenceId] =
    addInternal(pattern, pattern.elements.flatMap(_.items.toList.sorted))

  override def findPossiblePatterns(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: Iterable[TransactionType]): Iterator[PatternType] = {
    assume {
      val times = sequence.map(_.time).toVector
      times.sorted == times
    }

    import one.off_by.sequence.mining.gsp.utils.CollectionDistinctUtils._

    val inputSequence = sequence.toList.flatMap(t => t.items.toList.sorted.map(x => (t.time, x)))
    findPossiblePatternsInternal(gspOptions, inputSequence)
      .distinct
  }

  protected[gsp] override def addInternal(
    pattern: PatternType,
    iterable: Iterable[ItemType]
  ): HashTree[ItemType, TimeType, DurationType, SequenceId] = {
    val hash = getHash(iterable.head)
    children.get(hash) match {
      case Some(node) =>
        HashTreeNode[ItemType, TimeType, DurationType, SequenceId](
          depth = depth,
          children = children.updated(hash, node.addInternal(pattern, iterable.tail)))

      case None =>
        HashTreeNode[ItemType, TimeType, DurationType, SequenceId](
          depth = depth,
          children = children.updated(
            hash,
            HashTreeLeaf[ItemType, TimeType, DurationType, SequenceId](depth + 1, Vector(pattern))))
    }
  }

  protected[gsp] override def findPossiblePatternsInternal(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: List[(TimeType, ItemType)]): Iterator[PatternType] = {
    (if (depth == 0) {
      new ItemWithSequenceIterator(sequence, gspOptions)
    } else {
      val (first, inputSequences) = (sequence.head, sequence.tail)

      new ItemWithSequenceIterator(inputSequences, gspOptions) filter { case (item, _) =>
        gapFilterFunction(gspOptions, first._1, item._1)
      }
    }) flatMap { case (item, rest) =>
      children.get(getHash(item._2)).fold(Iterator.empty: Iterator[PatternType]) { node =>
        node.findPossiblePatternsInternal(gspOptions, rest)
      }
    }
  }

  private def gapFilterFunction(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    first: TimeType,
    item: TimeType
  ): Boolean = gspOptions.fold(true) { options =>
    val typeSupport = options.typeSupport

    options.minGap.fold(true) { minGap =>
      filterMinGap(item, first, minGap, typeSupport.timeAdd)
    } && options.maxGap.fold(true) { maxGap =>
      filterMaxGap(item, first, maxGap, typeSupport.timeAdd)
    }
  }

  private def filterMinGap(
    item: TimeType,
    first: TimeType,
    minGap: DurationType,
    timeAdd: (TimeType, DurationType) => TimeType): Boolean =
    implicitly[Ordering[TimeType]].gteq(item, timeAdd(first, minGap))

  private def filterMaxGap(
    item: TimeType,
    first: TimeType,
    maxGap: DurationType,
    timeAdd: (TimeType, DurationType) => TimeType): Boolean =
    implicitly[Ordering[TimeType]].lteq(item, timeAdd(first, maxGap))
}

private final class ItemWithSequenceIterator[
ItemType,
@specialized(Int, Long) TimeType: Ordering,
@specialized(Int, Long) DurationType
](
  private[this] var current: List[(TimeType, ItemType)],
  maybeOptions: Option[GSPOptions[TimeType, DurationType]]
) extends Iterator[((TimeType, ItemType), List[(TimeType, ItemType)])] {

  private[this] var previous = current

  override def hasNext: Boolean = current.nonEmpty

  override def next(): ((TimeType, ItemType), List[(TimeType, ItemType)]) = {
    val result = (current.head, pullUpPrevious())
    current = current.tail
    result
  }

  private[this] def pullUpPrevious(): List[(TimeType, ItemType)] = {
    val zeroTime = current.head._1
    val target = maybeOptions map { options =>
      options.windowSize.fold(zeroTime) { windowSize =>
        options.typeSupport.timeSubtract(zeroTime, windowSize)
      }
    } getOrElse zeroTime

    while (implicitly[Ordering[TimeType]].lt(previous.head._1, target)) {
      previous = previous.tail
    }

    previous
  }
}

private[gsp] final case class HashTreeLeaf[
ItemType: Ordering,
@specialized(Int, Long) TimeType: Ordering,
@specialized(Int, Long) DurationType,
SequenceId](
  depth: Int = 0,
  items: Vector[Pattern[ItemType]] = Vector()
) extends HashTree[ItemType, TimeType, DurationType, SequenceId] {

  import HashTreeUtils._

  override def add(pattern: PatternType): HashTree[ItemType, TimeType, DurationType, SequenceId] =
    if (items.length < maxLeafSize || getLength(pattern) == depth)
      HashTreeLeaf[ItemType, TimeType, DurationType, SequenceId](depth, pattern +: items)
    else {
      def prepareIterable(p: PatternType) =
        p.elements.flatMap(_.items.toList.sorted).drop(depth)

      val node: HashTree[ItemType, TimeType, DurationType, SequenceId] =
        HashTreeNode[ItemType, TimeType, DurationType, SequenceId](
          depth,
          HashMap.empty[Int, SelfType]).addInternal(pattern, prepareIterable(pattern))
      (node /: items) { case (tree, item) =>
        tree.addInternal(item, prepareIterable(item))
      }
    }

  override def findPossiblePatterns(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: Iterable[TransactionType]): Iterator[PatternType] = items.iterator

  protected[gsp] override def addInternal(
    pattern: PatternType,
    iterable: Iterable[ItemType]
  ): HashTree[ItemType, TimeType, DurationType, SequenceId] = add(pattern)

  protected[gsp] override def findPossiblePatternsInternal(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: List[(TimeType, ItemType)]): Iterator[PatternType] = items.iterator
}

private object HashTreeUtils {
  val maxLeafSize: Int = 8

  @inline
  def getLength[ItemType](pattern: Pattern[ItemType]): Int =
    pattern.elements.view.map(_.items.size).sum

  @inline
  def getItem[ItemType: Ordering](pattern: Pattern[ItemType], index: Int): ItemType =
    pattern.elements.view.flatMap(_.items.toList.sorted).apply(index)

  @inline
  def getHash[ItemType](item: ItemType): Int =
    item.hashCode()
}
