package one.off_by.sequence.mining.gsp.utils

sealed trait ForceSpecializer[@specialized T]

final case class ForceSpecializedValue[@specialized T]() extends ForceSpecializer[T]
