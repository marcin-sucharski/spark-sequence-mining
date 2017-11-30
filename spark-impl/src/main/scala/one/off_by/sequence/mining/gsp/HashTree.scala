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
    sequence: Iterable[TransactionType]): Iterable[PatternType]

  protected[gsp] def addInternal(
    pattern: PatternType,
    iterable: Iterable[ItemType]
  ): SelfType

  protected[gsp] def findPossiblePatternsInternal(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: Iterable[(TimeType, ItemType)]): Iterable[PatternType]
}

private[gsp] object HashTree {
  def empty[ItemType: Ordering, TimeType: Ordering, DurationType, SequenceId]
  : HashTree[ItemType, TimeType, DurationType, SequenceId] =
    HashTreeLeaf[ItemType, TimeType, DurationType, SequenceId](0, Vector())
}

private[gsp] case class HashTreeNode[ItemType: Ordering, TimeType: Ordering, DurationType, SequenceId](
  depth: Int,
  children: HashMap[Int, HashTree[ItemType, TimeType, DurationType, SequenceId]]
) extends HashTree[ItemType, TimeType, DurationType, SequenceId] {

  import HashTreeUtils._

  override def add(pattern: PatternType): SelfType =
    addInternal(pattern, pattern.elements.flatMap(_.items.toList.sorted))

  override def findPossiblePatterns(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: Iterable[TransactionType]): Iterable[PatternType] = {
    assume {
      val times = sequence.map(_.time).toVector
      times.sorted == times
    }

    findPossiblePatternsInternal(gspOptions, sequence.flatMap(t => t.items.toList.sorted.map(x => (t.time, x))))
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
          HashTree.empty[ItemType, TimeType, DurationType, SequenceId].addInternal(pattern, iterable)))
    }
  }

  protected[gsp] override def findPossiblePatternsInternal(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: Iterable[(TimeType, ItemType)]): Iterable[PatternType] = {
    def sequencesWithTail[T](in: Iterable[T]): Stream[(T, List[T])] = {
      in match {
        case x :: Nil  => Stream((x, Nil))
        case x :: rest =>
          lazy val next = sequencesWithTail(rest)
          (x, rest) #:: next
      }
    }

    if (depth == 0) {
      sequencesWithTail(sequence) flatMap { case (item, rest) =>
        children.get(item._2.hashCode()) match {
          case Some(node) => node.findPossiblePatternsInternal(gspOptions, item :: rest)
          case None       => Nil
        }
      }
    } else {
      val first :: inputSequences = sequence

      sequencesWithTail(inputSequences) filter { case (item, _) =>
        gspOptions.fold(true) { options =>
          val typeSupport = options.typeSupport

          val filterMinGap = options.minGap.fold(true) { minGap =>
            implicitly[Ordering[TimeType]].gteq(item._1, typeSupport.timeAdd(first._1, minGap))
          }
          val filterMaxGap = options.maxGap.fold(true) { maxGap =>
            implicitly[Ordering[TimeType]].lteq(item._1, typeSupport.timeAdd(first._1, maxGap))
          }

          filterMaxGap && filterMinGap
        }
      } flatMap { case (item, rest) =>
        children.get(item.hashCode()) match {
          case Some(node) => node.findPossiblePatternsInternal(gspOptions, item :: rest)
          case None       => Nil
        }
      }
    }
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
    sequence: Iterable[TransactionType]): Iterable[PatternType] = items

  protected[gsp] override def addInternal(
    pattern: PatternType,
    iterable: Iterable[ItemType]
  ): SelfType = add(pattern)

  protected[gsp] override def findPossiblePatternsInternal(
    gspOptions: Option[GSPOptions[TimeType, DurationType]],
    sequence: Iterable[(TimeType, ItemType)]): Iterable[PatternType] = items
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
