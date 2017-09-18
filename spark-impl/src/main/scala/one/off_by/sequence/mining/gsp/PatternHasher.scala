package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.Domain.Pattern

@specialized
trait PatternHasher[ItemType] {

  import Domain._

  /**
    * @return Hash for empty pattern.
    */
  def nil: Hash[ItemType]

  /**
    * Calculates hash of specified pattern.
    *
    * @param in Patter to be hashed.
    * @return Hash of the pattern.
    */
  def hash(in: Pattern[ItemType]): Hash[ItemType]

  /**
    * Generates hash for pattern from which input hash was calculated with
    * additional element on the beginning.
    *
    * @param element Additional element to be appended on the beginning.
    * @param hash    Hash of source pattern.
    * @return Hash of source pattern with additional element on the beginning.
    */
  def appendLeft(element: Element[ItemType], hash: Hash[ItemType]): Hash[ItemType]
}

case class Hash[ItemType](
  value: Int
) extends AnyVal

object PatternHasher {

  import Domain._

  /**
    * Represents pattern with cached hashes for prefix and suffix elements.
    *
    * Note the difference that hash is for prefix/suffix elements, not items.
    */
  @specialized
  case class PatternWithHash[ItemType](
    pattern: Pattern[ItemType],
    hash: Hash[ItemType],
    lastElement: Element[ItemType],
    prefixElementsHash: Hash[ItemType],
    suffixElementsHash: Hash[ItemType]
  )

  @specialized
  implicit class HashSupport[ItemType](
    hash: Hash[ItemType]
  )(implicit hasher: PatternHasher[ItemType]) {
    def ##:(element: Element[ItemType]): Hash[ItemType] = hasher.appendLeft(element, hash)
  }

  @specialized
  implicit class PatternSupport[ItemType](
    pattern: Pattern[ItemType]
  )(implicit hasher: PatternHasher[ItemType]) {
    def rawHash: Hash[ItemType] = hasher.hash(pattern)

    def hash: PatternWithHash[ItemType] = {
      require(pattern.elements.nonEmpty)

      val prefixHash = Pattern(pattern.elements.view.take(pattern.elements.size - 1)).rawHash
      val suffixHash = Pattern(pattern.elements.view.drop(1)).rawHash
      val patternHash = pattern.elements.head ##: suffixHash
      PatternWithHash(pattern, patternHash, pattern.elements.last, prefixHash, suffixHash)
    }
  }

  @specialized
  implicit class PatternWithHashSupport[ItemType](
    pattern: PatternWithHash[ItemType]
  )(implicit hasher: PatternHasher[ItemType]) {
    def prefixes: Seq[PatternWithHash[ItemType]] = {
      val possibleLastElements = allSubsetsWithoutSingleElement(pattern.lastElement.items).map(Element(_))
      possibleLastElements map { lastElement =>
        val newPattern = Pattern(pattern.pattern.elements.view.take(pattern.pattern.elements.size - 1) :+ lastElement)
        PatternWithHash(
          newPattern,
          ???,
          lastElement,
          pattern.prefixElementsHash,
          Pattern(newPattern.elements.drop(1)).rawHash
        )
      }
    }

    def suffixes: Seq[PatternWithHash[ItemType]] = {
      val possibleFirstElements = allSubsetsWithoutSingleElement(pattern.pattern.elements.head.items).map(Element(_))
      possibleFirstElements map { firstElement =>
        val newPattern = Pattern(firstElement +: pattern.pattern.elements.drop(1))
        PatternWithHash(
          newPattern,
          firstElement ##: pattern.suffixElementsHash,
          pattern.lastElement,
          ???,
          pattern.suffixElementsHash
        )
      }
    }

    private def allSubsetsWithoutSingleElement(set: Set[ItemType]): Seq[Set[ItemType]] =
      set.toSeq.map(set - _)
  }

}

@specialized
class DefaultPatternHasher[ItemType] extends PatternHasher[ItemType] {
  override def nil: Hash[ItemType] = Hash(0)

  override def hash(in: Pattern[ItemType]): Hash[ItemType] =
    (nil /: in.elements.view.reverse) { case (hash, element) => appendLeft(element, hash) }

  override def appendLeft(element: Domain.Element[ItemType], hash: Hash[ItemType]): Hash[ItemType] =
    Hash[ItemType](element.## ^ Integer.rotateLeft(hash.value, 1))
}
