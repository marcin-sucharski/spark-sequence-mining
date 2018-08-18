package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.utils.LoggingUtils
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

private[gsp] class PatternJoiner[ItemType: Ordering](
  partitioner: Partitioner
) extends LoggingUtils {

  import PatternJoiner._

  def generateCandidates(patterns: RDD[Pattern[ItemType]]): RDD[Pattern[ItemType]] = {
    logger.trace(s"Generating candidates from: ${patterns.collect().mkString("\n", "\n", "\n")}")
    pruneMatches(joinPatterns(patterns), patterns)
  }

  private[gsp] def joinPatterns(patterns: RDD[Pattern[ItemType]]): RDD[Pattern[ItemType]] = {
    val isFirst = patterns.take(1).head.elements.map(_.items.size).sum == 1

    val prefixes = patterns
      .flatMap(_.prefixes)
      .map(p => (p.pattern, p))

    val suffixes = patterns
      .flatMap(_.suffixes)
      .map(p => (p.pattern, p))

    val initialData = joinSuffixesAndPrefixes(prefixes, suffixes, isFirst)
      .filter { case (prefix, suffix) =>
        isPrefixCompatibleWithSuffix(prefix, suffix)
      }

    val result = initialData map { case (prefix, suffix) =>
      buildPattern(prefix, suffix)
    }

    if (isFirst) result
    else result.distinct()
  }

  private def buildPattern(prefix: PrefixResult[ItemType], suffix: SuffixResult[ItemType]) = {
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

  private def isPrefixCompatibleWithSuffix(
    prefix: PrefixResult[ItemType],
    suffix: SuffixResult[ItemType]
  ) = {
    val common = prefix.pattern.elements
    (prefix.joinItem, suffix.joinItem) match {
      case (JoinItemNewElement(_), JoinItemNewElement(_)) =>
        true

      case (JoinItemExistingElement(suffixItem), JoinItemNewElement(_)) =>
        val newItemIsAlreadyInLastElement = common.lastOption.exists(_.items contains suffixItem)
        !newItemIsAlreadyInLastElement

      case (JoinItemNewElement(_), JoinItemExistingElement(prefixItem)) =>
        val newItemIsAlreadyInFirstElement = common.headOption.exists(_.items contains prefixItem)
        !newItemIsAlreadyInFirstElement

      case (JoinItemExistingElement(suffixItem), JoinItemExistingElement(prefixItem)) =>
        val newItemIsNotInFirstElement = common.headOption.exists(!_.items.contains(prefixItem))
        val newItemIsNotInLastElement = common.lastOption.exists(!_.items.contains(suffixItem))
        val newItemsAreNotEquivalent = common.length > 1 || prefixItem != suffixItem

        newItemIsNotInFirstElement && newItemIsNotInLastElement &&
          newItemsAreNotEquivalent
    }
  }

  private def joinSuffixesAndPrefixes(
    prefixes: RDD[(Pattern[ItemType], PatternJoiner.PrefixResult[ItemType])],
    suffixes: RDD[(Pattern[ItemType], PatternJoiner.SuffixResult[ItemType])],
    isFirst: Boolean
  ): RDD[(PatternJoiner.PrefixResult[ItemType], PatternJoiner.SuffixResult[ItemType])] = {
    if (isFirst) {
      val ordering = implicitly[Ordering[ItemType]]

      val rawPrefixes = prefixes.map(_._2)
      val rawSuffixes = suffixes.map(_._2)

      val newItemPrefixes = rawPrefixes.filter(_.pattern.elements.isEmpty)
      val newItemSuffixes = rawSuffixes.filter(_.pattern.elements.isEmpty)

      val joinItemPrefixes = rawPrefixes.filter(_.pattern.elements.nonEmpty)
      val joinItemSuffixes = rawSuffixes.filter(_.pattern.elements.nonEmpty)

      def process(
        initialPrefixes: RDD[PatternJoiner.PrefixResult[ItemType]],
        initialSuffixes: RDD[PatternJoiner.SuffixResult[ItemType]]
      ): RDD[(PatternJoiner.PrefixResult[ItemType], PatternJoiner.SuffixResult[ItemType])] =
        initialPrefixes.cartesian(initialSuffixes) filter { case (prefix, suffix) =>
          (prefix.joinItem, suffix.joinItem) match {
            case (JoinItemExistingElement(suffixItem), JoinItemExistingElement(prefixItem)) =>
              ordering.lteq(suffixItem, prefixItem)

            case _ =>
              true
          }
        }

      process(newItemPrefixes, newItemSuffixes)
        .union(process(joinItemPrefixes, joinItemSuffixes))
        .coalesce(partitioner.numPartitions * partitioner.numPartitions)
    } else prefixes join suffixes map (_._2)
  }

  private[gsp] def pruneMatches(
    matches: RDD[Pattern[ItemType]],
    source: RDD[Pattern[ItemType]]
  ): RDD[Pattern[ItemType]] = {
    logger.trace(s"Pruning matches: ${matches.collect().mkString("\n", "\n", "\n")}")

    val isFirst = source.take(1).head.elements.map(_.items.size).sum == 1

    if (isFirst) matches
    else pruneMatchesForPatternsLongerThanTwoItems(matches, source)
  }

  private def pruneMatchesForPatternsLongerThanTwoItems(
    matches: RDD[Pattern[ItemType]],
    source: RDD[Pattern[ItemType]]
  ): RDD[Pattern[ItemType]] = {
    val subsequencesWithoutSingleItem = matches
      .flatMap { pattern =>
        pattern.elements.indices flatMap { index =>
          allSubsetsWithoutSingleItem(pattern.elements(index).items)
            .map { case (_, element) => element }
            .map { newElement =>
              val newPattern = pattern.elements
                .updated(index, newElement)
                .filter(_.items.nonEmpty)
              (Pattern(newPattern), pattern)
            }
        }
      }

    logger.trace(s"Prune subsequences: ${subsequencesWithoutSingleItem.map(_._1).toPrettyList}")

    val countedSubsequences = source
      .map(p => (p, 1))
      .cogroup(subsequencesWithoutSingleItem)
      .flatMap { case (_, (counts, patterns)) =>
        if (counts.nonEmpty) patterns map (p => (p, 1))
        else Nil
      } reduceByKey(_ + _, partitioner.numPartitions * partitioner.numPartitions)

    logger.trace(s"Counted subsequences: ${countedSubsequences.toPrettyList}")

    countedSubsequences filter { case (pattern, count) =>
      val expectedCount = pattern.elements.view.map(_.items.size).sum
      count == expectedCount
    } map (_._1)
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
  case class JoinItemNewElement[ItemType](
    override val item: ItemType
  ) extends JoinItem[ItemType]

  /**
    * Item missing from pattern in prefix/suffix is member of existing element.
    */
  case class JoinItemExistingElement[ItemType](
    override val item: ItemType
  ) extends JoinItem[ItemType]

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

    def prepend(element: Element[ItemType]): Pattern[ItemType] =
      Pattern(element +: pattern.elements)
  }

  private[gsp] def allSubsetsWithoutSingleItem[T](set: Set[T]): Seq[(T, Element[T])] =
    set.toSeq.map(item => (item, Element(set - item)))
}
