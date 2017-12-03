package one.off_by.sequence.mining.gsp

import scala.collection.immutable.HashMap
import scala.language.postfixOps

private[gsp] sealed trait HashTree[ItemType, TimeType, DurationType, SequenceId] {
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

private[gsp] object HashTree {
  def empty[ItemType: Ordering, TimeType: Ordering, DurationType, SequenceId]
  : HashTree[ItemType, TimeType, DurationType, SequenceId] =
    HashTreeLeaf[ItemType, TimeType, DurationType, SequenceId](0, Vector())
}

private[gsp] case class HashTreeNode[ItemType: Ordering, TimeType: Ordering, DurationType, SequenceId](
  depth: Int,
  children: Map[Int, HashTree[ItemType, TimeType, DurationType, SequenceId]]
) extends HashTree[ItemType, TimeType, DurationType, SequenceId] {

  import HashTreeUtils._

  override def add(pattern: PatternType): SelfType =
    addInternal(pattern, pattern.elements.flatMap(_.items.toList.sorted))

  override def findPossiblePatterns(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: Iterable[TransactionType]): Iterator[PatternType] = {
    assume {
      val times = sequence.map(_.time).toVector
      times.sorted == times
    }

    findPossiblePatternsInternal(gspOptions, sequence.toList.flatMap(t => t.items.toList.sorted.map(x => (t.time, x))))
  }

  protected[gsp] override def addInternal(
    pattern: PatternType,
    iterable: Iterable[ItemType]
  ): SelfType = {
    val hash = getHash(iterable.head)
    children.get(hash) match {
      case Some(node) =>
        copy(children = children.updated(hash, node.addInternal(pattern, iterable.tail)))

      case None =>
        copy(children = children.updated(
          hash,
          HashTreeLeaf[ItemType, TimeType, DurationType, SequenceId](depth + 1, Vector(pattern))))
    }
  }

  protected[gsp] override def findPossiblePatternsInternal(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: List[(TimeType, ItemType)]): Iterator[PatternType] = {
    (if (depth == 0) {
      new ItemWithTailIterator(sequence)
    } else {
      val (first, inputSequences) = (sequence.head, sequence.tail)
      val filterFunction = gapFilterFunction(gspOptions, first._1)

      new ItemWithTailIterator(inputSequences) filter { case (item, _) =>
        filterFunction(item._1)
      }
    }) flatMap { case (item, rest) =>
      children.get(getHash(item._2)) match {
        case Some(node) => node.findPossiblePatternsInternal(gspOptions, item :: rest)
        case None       => Nil
      }
    }
  }

  private def gapFilterFunction(
    gspOptions: Option[GSPOptions[TimeType, DurationType]], first: TimeType
  ): (TimeType) => Boolean = gspOptions.fold((_: TimeType) => true) { options =>
    val typeSupport = options.typeSupport

    def filterMinGap(item: TimeType) = options.minGap.fold(true) { minGap =>
      implicitly[Ordering[TimeType]].gteq(item, typeSupport.timeAdd(first, minGap))
    }
    def filterMaxGap(item: TimeType) = options.maxGap.fold(true) { maxGap =>
      implicitly[Ordering[TimeType]].lteq(item, typeSupport.timeAdd(first, maxGap))
    }

    (item: TimeType) => filterMinGap(item) && filterMaxGap(item)
  }
}

private class ItemWithTailIterator[T](private var current: List[T]) extends Iterator[(T, List[T])] {
  override def hasNext: Boolean = current.nonEmpty

  override def next(): (T, List[T]) = {
    val result = (current.head, current.tail)
    current = current.tail
    result
  }
}

private[gsp] case class HashTreeLeaf[ItemType: Ordering, TimeType: Ordering, DurationType, SequenceId](
  depth: Int,
  items: Vector[Pattern[ItemType]]
) extends HashTree[ItemType, TimeType, DurationType, SequenceId] {

  import HashTreeUtils._

  override def add(pattern: PatternType): SelfType =
    if (items.length < maxLeafSize || getLength(pattern) == depth) HashTreeLeaf(depth, pattern +: items)
    else {
      val node: HashTree[ItemType, TimeType, DurationType, SequenceId] =
        HashTreeNode[ItemType, TimeType, DurationType, SequenceId](
          depth,
          HashMap.empty[Int, SelfType])
      (node /: items) (_ add _)
    }

  override def findPossiblePatterns(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: Iterable[TransactionType]): Iterator[PatternType] = items.iterator

  protected[gsp] override def addInternal(
    pattern: PatternType,
    iterable: Iterable[ItemType]
  ): SelfType = add(pattern)

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
