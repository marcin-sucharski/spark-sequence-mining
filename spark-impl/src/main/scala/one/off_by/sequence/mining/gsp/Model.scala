package one.off_by.sequence.mining.gsp

case class Transaction[
@specialized(Int, Long) ItemType,
@specialized(Int, Long) TimeType,
SequenceId
](
  sequenceId: SequenceId,
  time: TimeType,
  items: Set[ItemType]
) {
  assume(items.nonEmpty)
}

case class Element[@specialized(Int, Long) ItemType](
  items: Set[ItemType]
) {
  def +(item: ItemType): Element[ItemType] =
    Element(items + item)

  override def toString: String = items.mkString("(", ", ", ")")
}

object Element {
  def apply[@specialized(Int, Long) ItemType](items: ItemType*): Element[ItemType] =
    Element(Set(items: _*))
}

case class Pattern[@specialized(Int, Long) ItemType](
  elements: Vector[Element[ItemType]]
) {
  override def toString: String = elements.mkString("<", "", ">")

  override lazy val hashCode: Int = elements.hashCode()
}

object Domain {
  type Percent = Double
  type Support = Percent
  type SupportCount = Int
}