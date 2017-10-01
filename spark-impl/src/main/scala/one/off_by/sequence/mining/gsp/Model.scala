package one.off_by.sequence.mining.gsp

@specialized
case class Transaction[ItemType, TimeType, SequenceId](
  sequenceId: SequenceId,
  time: TimeType,
  items: Set[ItemType]
)

@specialized
case class Taxonomy[ItemType](
  ancestor: ItemType,
  descendants: List[ItemType]
)

@specialized
case class Element[ItemType](
  items: Set[ItemType]
) extends AnyVal {
  def +(item: ItemType): Element[ItemType] =
    Element(items + item)
}

object Element {
  def apply[ItemType](items: ItemType*): Element[ItemType] =
    Element(Set(items: _*))
}

@specialized
case class Pattern[ItemType](
  elements: Vector[Element[ItemType]]
) extends AnyVal

object Domain {
  type Percent = Double
  type Support = Percent
  type SupportCount = Long
}