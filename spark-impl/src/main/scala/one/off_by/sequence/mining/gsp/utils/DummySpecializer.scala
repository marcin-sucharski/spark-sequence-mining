package one.off_by.sequence.mining.gsp.utils

sealed trait DummySpecializer[@specialized T]

final case class DummySpecializedValue[@specialized T]() extends DummySpecializer[T]
