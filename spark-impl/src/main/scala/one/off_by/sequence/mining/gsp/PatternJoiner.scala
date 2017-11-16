package one.off_by.sequence.mining.gsp

import grizzled.slf4j.Logging
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

//TODO: check without PatternHasher
private[gsp] class PatternJoiner[ItemType: Ordering](
  partitioner: Partitioner
) extends Logging {

  import PatternJoiner._

  def generateCandidates(patterns: RDD[Pattern[ItemType]]): RDD[Pattern[ItemType]] = {
    patterns.cache()
    pruneMatches(joinPatterns(patterns), patterns)
  }

  private[gsp] def joinPatterns(patterns: RDD[Pattern[ItemType]]): RDD[Pattern[ItemType]] = {
    val ordering = implicitly[Ordering[ItemType]]

    val prefixes = patterns
      .flatMap(_.prefixes)
      .map(p => (p.pattern, p))
      .partitionBy(partitioner)

    val suffixes = patterns
      .flatMap(_.suffixes)
      .map(p => (p.pattern, p))

    val initialData = prefixes join suffixes filter { case (_, (prefix, suffix)) =>
      val common = prefix.pattern.elements
      val itemsCount = common.map(_.items.size).sum
      (prefix.joinItem, suffix.joinItem) match {
        case (JoinItemNewElement(suffixItem), JoinItemNewElement(prefixItem)) =>
          prefixItem != suffixItem || common.length > 1

        case (JoinItemExistingElement(suffixItem), JoinItemNewElement(_)) =>
          !common.lastOption.exists(_.items contains suffixItem) && itemsCount > 1

        case (JoinItemNewElement(_), JoinItemExistingElement(prefixItem)) =>
          !common.headOption.exists(_.items contains prefixItem)

        case (JoinItemExistingElement(suffixItem), JoinItemExistingElement(prefixItem)) =>
          common.headOption.exists(!_.items.contains(prefixItem)) &&
            common.lastOption.exists(!_.items.contains(suffixItem)) &&
            (common.length > 1 || prefixItem != suffixItem)
      }
    } map (_._2)
    initialData.cache()

    val candidateBuilders = (initialData filter { case (prefix, suffix) =>
      (prefix.joinItem, suffix.joinItem) match {
        case (JoinItemExistingElement(suffixItem), JoinItemExistingElement(prefixItem)) => suffixItem == prefixItem
        case _                                                                          => false
      }
    } distinct()) union (initialData filter { case (prefix, suffix) =>
      (prefix.joinItem, suffix.joinItem) match {
        case (JoinItemExistingElement(suffixItem), JoinItemExistingElement(prefixItem)) =>
          prefix.pattern.elements.length > 1 || ordering.lt(suffixItem, prefixItem)

        case _ =>
          true
      }
    })

    val result = candidateBuilders map { case (prefix, suffix) =>
      val common = prefix.pattern.elements
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
    }

    val isFirst = patterns.take(1).head.elements.map(_.items.size).sum == 1

    if (isFirst) result
    else result.distinct()
  }

  private[gsp] def pruneMatches(
    matches: RDD[Pattern[ItemType]],
    source: RDD[Pattern[ItemType]]
  ): RDD[Pattern[ItemType]] = {

    val subsequences = matches
      .flatMap { pattern =>
        pattern.elements.indices flatMap { index =>
          if (pattern.elements(index).items.size > 1) {
            allSubsetsWithoutSingleItem(pattern.elements(index).items).map(_._2) map { newElement =>
              val result = Pattern(pattern.elements.updated(index, newElement))
              (result, pattern)
            }
          } else Nil
        }
      }

    source map (p => (p, 1)) join subsequences map { case (_, (count, pattern)) =>
      (pattern, count)
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

  /**
    * Describes missing item from pattern in prefix/suffix.
    */
  sealed trait JoinItem[ItemType] {
    def item: ItemType
  }

  /**
    * Item missing from pattern in prefix/suffix creates new element.
    */
  case class JoinItemNewElement[ItemType](override val item: ItemType) extends JoinItem[ItemType]

  /**
    * Item missing from pattern in prefix/suffix is member of existing element.
    */
  case class JoinItemExistingElement[ItemType](override val item: ItemType) extends JoinItem[ItemType]

  sealed trait PrefixSuffixResult[ItemType] {
    def pattern: Pattern[ItemType]

    def joinItem: JoinItem[ItemType]
  }

  case class PrefixResult[ItemType](
    pattern: Pattern[ItemType],
    joinItem: JoinItem[ItemType]
  ) extends PrefixSuffixResult[ItemType]

  case class SuffixResult[ItemType](
    pattern: Pattern[ItemType],
    joinItem: JoinItem[ItemType]
  ) extends PrefixSuffixResult[ItemType]

  implicit class PatternSupport[ItemType](
    pattern: Pattern[ItemType]
  ) {

    def prefixes: Seq[PrefixResult[ItemType]] = {
      assume(pattern.elements.nonEmpty && pattern.elements.head.items.nonEmpty)
      impl(pattern.elements.last, pattern.elements.take(pattern.elements.size - 1), _ :+ _, PrefixResult(_, _))
    }

    def suffixes: Seq[SuffixResult[ItemType]] = {
      assume(pattern.elements.nonEmpty && pattern.elements.head.items.nonEmpty)
      impl(pattern.elements.head, pattern.elements.tail, _ prepend _, SuffixResult(_, _))
    }

    @inline
    private def impl[ResultType <: PrefixSuffixResult[ItemType]](
      targetElement: Element[ItemType],
      withoutTarget: Vector[Element[ItemType]],
      addLast: (Pattern[ItemType], Element[ItemType]) => Pattern[ItemType],
      makeResult: (Pattern[ItemType], JoinItem[ItemType]) => ResultType
    ): Seq[ResultType] = {
      val singleItemPattern = pattern.elements.size == 1 && pattern.elements.head.items.size == 1
      val withoutTargetPattern = Pattern(withoutTarget)
      val possibleLastElements = allSubsetsWithoutSingleItem(targetElement.items)
      possibleLastElements flatMap {
        case (item, lastElement) if lastElement.items.isEmpty =>
          if (singleItemPattern) {
            val singleElementPattern = Pattern(Vector(Element[ItemType]()))
            val emptyPattern = Pattern(Vector[Element[ItemType]]())
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

    def :+(element: Element[ItemType]): Pattern[ItemType] =
      Pattern(pattern.elements :+ element)

    def append(element: Element[ItemType]): Pattern[ItemType] =
      this :+ element

    def prepend(element: Element[ItemType]): Pattern[ItemType] =
      Pattern(element +: pattern.elements)
  }

  private[gsp] def allSubsetsWithoutSingleItem[T](set: Set[T]): Seq[(T, Element[T])] =
    set.toSeq.map(item => (item, Element(set - item)))
}
