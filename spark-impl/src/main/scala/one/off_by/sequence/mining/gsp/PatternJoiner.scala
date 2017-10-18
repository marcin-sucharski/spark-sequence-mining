package one.off_by.sequence.mining.gsp

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

@specialized
private[gsp] class PatternJoiner[ItemType](
  hasher: Broadcast[PatternHasher[ItemType]],
  partitioner: Partitioner
) {

  import PatternJoiner._

  def generateCandidates(patterns: RDD[Pattern[ItemType]]): RDD[Pattern[ItemType]] = {
    pruneMatches(joinPatterns(patterns), patterns)
  }

  private[gsp] def joinPatterns(patterns: RDD[Pattern[ItemType]]): RDD[Pattern[ItemType]] = {
    val localHasher = hasher

    val prefixes = patterns
      .flatMap { pattern =>
        implicit val hasher: PatternHasher[ItemType] = localHasher.value
        pattern.prefixes
      }
      .map { p =>
        implicit val hasher: PatternHasher[ItemType] = localHasher.value
        (p.pattern.hash, p)
      }
      .partitionBy(partitioner)

    val suffixes = patterns
      .flatMap { pattern =>
        implicit val hasher: PatternHasher[ItemType] = localHasher.value
        pattern.suffixes
      }
      .map(p => (p.pattern.hash, p))

    prefixes join suffixes filter { case (_, (prefix, suffix)) =>
      prefix.pattern.pattern == suffix.pattern.pattern
    } filter { case (_, (prefix, suffix)) =>
      val common = prefix.pattern.pattern.elements
      (prefix.joinItem, suffix.joinItem) match {
        case (JoinItemNewElement(suffixItem), JoinItemNewElement(prefixItem)) =>
          prefixItem != suffixItem || common.length > 1

        case (JoinItemExistingElement(suffixItem), JoinItemNewElement(_)) =>
          !common.lastOption.exists(_.items contains suffixItem)

        case (JoinItemNewElement(_), JoinItemExistingElement(prefixItem)) =>
          !common.headOption.exists(_.items contains prefixItem)

        case (JoinItemExistingElement(suffixItem), JoinItemExistingElement(prefixItem)) =>
          common.headOption.exists(!_.items.contains(prefixItem)) &&
            common.lastOption.exists(!_.items.contains(suffixItem)) &&
            (common.length > 1 || prefixItem != suffixItem)
      }
    } map { case (_, (prefix, suffix)) =>
      val common = prefix.pattern.pattern.elements
      val lastIndex = common.size - 1
      val elements = (prefix.joinItem, suffix.joinItem) match {
        case (JoinItemNewElement(suffixItem), JoinItemNewElement(prefixItem)) =>
          Element(prefixItem) +: common :+ Element(suffixItem)

        case (JoinItemExistingElement(suffixItem), JoinItemNewElement(prefixItem)) =>
          Element(prefixItem) +: common.updated(lastIndex, common(lastIndex) + suffixItem)

        case (JoinItemNewElement(suffixItem), JoinItemExistingElement(prefixItem)) =>
          common.updated(0, common(0) + prefixItem) :+ Element(suffixItem)

        case (JoinItemExistingElement(suffixItem), JoinItemExistingElement(prefixItem)) =>
          val withPrefix = common.updated(0, common(0) + prefixItem)
          withPrefix.updated(lastIndex, withPrefix(lastIndex) + suffixItem)
      }
      Pattern(elements)
    } distinct()
  }

  private[gsp] def pruneMatches(
    matches: RDD[Pattern[ItemType]],
    source: RDD[Pattern[ItemType]]
  ): RDD[Pattern[ItemType]] = {
    import PatternHasher._

    val localHasher = hasher

    source.cache()
    val sourceWithHash = source map { pattern =>
      implicit val hasher: PatternHasher[ItemType] = localHasher.value
      val result = pattern.hash
      (result.hash, result)
    } partitionBy partitioner

    val subsequences = matches
      .flatMap { pattern =>
        implicit val hasher: PatternHasher[ItemType] = localHasher.value

        pattern.elements.indices flatMap { index =>
          if (pattern.elements(index).items.size > 1) {
            allSubsetsWithoutSingleItem(pattern.elements(index).items).map(_._2) map { newElement =>
              val result = Pattern(pattern.elements.updated(index, newElement)).hash
              (result.hash, (pattern, result))
            }
          } else Nil
        }
      }

    sourceWithHash join subsequences filter { case (_, (sourcePattern, (_, subsequencePattern))) =>
      sourcePattern == subsequencePattern
    } map { case (_, (_, (matchedPattern, _))) =>
      (matchedPattern, 1)
    } reduceByKey(partitioner, _ + _) filter { case (pattern, count) =>
      val expectedCount = pattern.elements.map(elements => {
        if (elements.items.size > 1) elements.items.size
        else 0
      }).sum
      count == expectedCount
    } map(_._1) union matches.filter(p => p.elements.size == 2 && p.elements.forall(_.items.size == 1))
  }
}

private[gsp] object PatternJoiner {

  import PatternHasher._

  /**
    * Describes missing item from pattern in prefix/suffix.
    */
  @specialized
  sealed trait JoinItem[ItemType] {
    def item: ItemType
  }

  /**
    * Item missing from pattern in prefix/suffix creates new element.
    */
  @specialized
  case class JoinItemNewElement[ItemType](override val item: ItemType) extends JoinItem[ItemType]

  /**
    * Item missing from pattern in prefix/suffix is member of existing element.
    */
  @specialized
  case class JoinItemExistingElement[ItemType](override val item: ItemType) extends JoinItem[ItemType]

  @specialized
  sealed trait PrefixSuffixResult[ItemType] {
    def pattern: PatternWithHash[ItemType]

    def joinItem: JoinItem[ItemType]
  }

  @specialized
  case class PrefixResult[ItemType](
    pattern: PatternWithHash[ItemType],
    joinItem: JoinItem[ItemType]
  ) extends PrefixSuffixResult[ItemType]

  @specialized
  case class SuffixResult[ItemType](
    pattern: PatternWithHash[ItemType],
    joinItem: JoinItem[ItemType]
  ) extends PrefixSuffixResult[ItemType]

  @specialized
  implicit class PatternWithHashSupport[ItemType](
    pattern: Pattern[ItemType]
  )(implicit hasher: PatternHasher[ItemType]) {
    require(pattern.elements.nonEmpty && pattern.elements.head.items.nonEmpty)

    def prefixes: Seq[PrefixResult[ItemType]] =
      impl(pattern.elements.last, pattern.elements.take(pattern.elements.size - 1), _ :+ _, PrefixResult(_, _))

    def suffixes: Seq[SuffixResult[ItemType]] =
      impl(pattern.elements.head, pattern.elements.tail, _ prepend _, SuffixResult(_, _))

    @inline
    @specialized
    private def impl[ResultType <: PrefixSuffixResult[ItemType]](
      targetElement: Element[ItemType],
      withoutTarget: Vector[Element[ItemType]],
      addLast: (PatternWithHash[ItemType], Element[ItemType]) => PatternWithHash[ItemType],
      makeResult: (PatternWithHash[ItemType], JoinItem[ItemType]) => ResultType
    ): Seq[ResultType] = {
      val singleItemPattern = pattern.elements.size == 1 && pattern.elements.head.items.size == 1
      val withoutTargetPattern = Pattern(withoutTarget).hash
      val possibleLastElements = allSubsetsWithoutSingleItem(targetElement.items)
      possibleLastElements flatMap {
        case (item, lastElement) if lastElement.items.isEmpty =>
          if (singleItemPattern) {
            val singleElementPattern = Pattern(Vector(Element[ItemType]())).hash
            val emptyPattern = Pattern(Vector[Element[ItemType]]()).hash
            List(
              makeResult(singleElementPattern, JoinItemExistingElement(item)),
              makeResult(emptyPattern, JoinItemNewElement(item))
            )
          }
          else {
            makeResult(withoutTargetPattern, JoinItemNewElement(item)) :: Nil
          }

        case (item, lastElement) =>
          makeResult(addLast(withoutTargetPattern, lastElement), JoinItemExistingElement(item)) :: Nil
      }
    }
  }

  @specialized
  private[gsp] def allSubsetsWithoutSingleItem[T](set: Set[T]): Seq[(T, Element[T])] =
    set.toSeq.map(item => (item, Element(set - item)))
}
