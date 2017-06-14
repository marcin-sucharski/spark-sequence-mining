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
