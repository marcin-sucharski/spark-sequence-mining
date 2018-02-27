package one.off_by.sequence.mining.gsp

case class Transaction[
@specialized(Int, Long, Float, Double) ItemType,
@specialized(Int, Long, Float, Double) TimeType,
SequenceId
](
  sequenceId: SequenceId,
  time: TimeType,
  items: Set[ItemType]
) {
  assume(items.nonEmpty)
}

case class Element[@specialized(Int, Long, Float, Double) ItemType](
  items: Set[ItemType]
) {
  def +(item: ItemType): Element[ItemType] =
    Element(items + item)

  override def toString = items.mkString("(", ", ", ")")
}

object Element {
  def apply[@specialized(Int, Long, Float, Double) ItemType](items: ItemType*): Element[ItemType] =
    Element(Set(items: _*))
}

case class Pattern[@specialized(Int, Long, Float, Double) ItemType](
  elements: Vector[Element[ItemType]]
) {
  override def toString = elements.mkString("<", "", ">")

  override lazy val hashCode: Int = elements.hashCode()
}

object Domain {
  type Percent = Double
  type Support = Percent
  type SupportCount = Int
}