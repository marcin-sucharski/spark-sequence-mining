package one.off_by.sequence.mining.gsp

case class Transaction[ItemType, TimeType, SequenceId](
  sequenceId: SequenceId,
  time: TimeType,
  items: Set[ItemType]
) {
  assume(items.nonEmpty)
}

case class Taxonomy[ItemType](
  ancestor: ItemType,
  descendants: List[ItemType]
)

case class Element[ItemType](
  items: Set[ItemType]
) {
  def +(item: ItemType): Element[ItemType] =
    Element(items + item)

  override def toString = items.mkString("(", ", ", ")")
}

object Element {
  def apply[ItemType](items: ItemType*): Element[ItemType] =
    Element(Set(items: _*))
}

case class Pattern[ItemType](
  elements: Vector[Element[ItemType]]
) {
  override def toString = elements.mkString("<", "", ">")

  override lazy val hashCode: Int = elements.hashCode()
}

object Domain {
  type Percent = Double
  type Support = Percent
  type SupportCount = Long
}