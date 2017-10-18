package one.off_by.testkit

import one.off_by.sequence.mining.gsp.{DefaultPatternHasher, PatternHasher}

trait DefaultPatternHasherHelper {
  @specialized
  protected def withHasher[ItemType](f: PatternHasher[ItemType] => Unit): Unit =
    f(new DefaultPatternHasher[ItemType]())
}
