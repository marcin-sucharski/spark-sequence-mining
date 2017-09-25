package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.Domain.{Element, Pattern}

@specialized
trait PatternHasher[ItemType] extends Serializable {

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
  def hash(in: Pattern[ItemType]): Hash[ItemType] =
    hashSeq(in.elements)

  /**
    * Generates hash for pattern from which input hash was calculated with
    * additional element on the end.
    *
    * @param hash    Hash of source pattern.
    * @param element Additional element to be appended on the end.
    * @return Hash of source pattern with additional element on the end.
    */
  def appendRight(hash: Hash[ItemType], element: Element[ItemType]): Hash[ItemType]

  def hashSeq(in: Seq[Element[ItemType]]): Hash[ItemType]
}

case class Hash[ItemType](
  value: Int
) extends AnyVal

object PatternHasher {

  import Domain._

  @specialized
  case class PatternWithHash[ItemType](
    pattern: Pattern[ItemType],
    hash: Hash[ItemType]
  )

  @specialized
  implicit class HashSupport[ItemType](
    hash: Hash[ItemType]
  )(implicit hasher: PatternHasher[ItemType]) {
    def :##(element: Element[ItemType]): Hash[ItemType] = hasher.appendRight(hash, element)
  }

  @specialized
  implicit class PatternSupport[ItemType](
    pattern: Pattern[ItemType]
  )(implicit hasher: PatternHasher[ItemType]) {
    def rawHash: Hash[ItemType] = hasher.hash(pattern)

    def hash: PatternWithHash[ItemType] =
      PatternWithHash(pattern, pattern.rawHash)
  }

  @specialized
  implicit class PatternWithHashSupport[ItemType](
    pattern: PatternWithHash[ItemType]
  )(implicit hasher: PatternHasher[ItemType]) {
    def :+(element: Element[ItemType]): PatternWithHash[ItemType] =
      PatternWithHash(Pattern(pattern.pattern.elements :+ element), pattern.hash :## element)

    def append(element: Element[ItemType]): PatternWithHash[ItemType] =
      this :+ element

    def prepend(element: Element[ItemType]): PatternWithHash[ItemType] =
      Pattern(element +: pattern.pattern.elements).hash
  }
}

@specialized
class DefaultPatternHasher[ItemType] extends PatternHasher[ItemType] {
  override def nil: Hash[ItemType] = Hash(0)

  override def hashSeq(in: Seq[Element[ItemType]]): Hash[ItemType] =
    (nil /: in)(appendRight)

  override def appendRight(hash: Hash[ItemType], element: Domain.Element[ItemType]): Hash[ItemType] =
    Hash[ItemType](Integer.rotateLeft(hash.value, 1) ^ element.##)
}
