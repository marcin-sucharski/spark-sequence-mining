package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.Domain.{Element, Pattern}

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

    private def allSubsetsWithoutSingleItem(set: Set[ItemType]): Seq[(ItemType, Element[ItemType])] =
      set.toSeq.map(item => (item, Element(set - item)))
  }
}
