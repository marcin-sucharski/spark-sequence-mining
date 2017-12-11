package one.off_by.sequence.mining.generator

import one.off_by.sequence.mining.gsp.{Element, Pattern, Transaction}

import scala.annotation.tailrec
import scala.language.postfixOps

object DataGenerator {
  case class Arguments(
    sequenceCount: Int,
    frequentSequenceElementCount: Int,
    frequentSequenceApproxSupport: Double)

  val defaultArguments = Arguments(
    sequenceCount = 10 * 1000,
    frequentSequenceElementCount = 50,
    frequentSequenceApproxSupport = 30.0)

  @tailrec
  def getArgs(in: Seq[String], acc: Arguments = defaultArguments): Arguments = in match {
    case Nil =>
      acc
    case "--sequence-count" :: sequenceCount :: rest =>
      getArgs(rest, acc.copy(sequenceCount = sequenceCount.toInt))
    case "--freq-sequence-element-count" :: freqSequenceCount :: rest =>
      getArgs(rest, acc.copy(frequentSequenceElementCount = freqSequenceCount.toInt))
    case "--freq-sequence-support" :: freqSequenceSupport :: rest =>
      getArgs(rest, acc.copy(frequentSequenceApproxSupport = freqSequenceSupport.toDouble))
    case other =>
      sys.error(s"invalid argument format: $other")
  }

  def main(args: Array[String]): Unit = {
    val arguments = getArgs(args.toList)
    val frequentSequenceCount = (arguments.sequenceCount * arguments.frequentSequenceApproxSupport / 100.0).toInt
    val pattern = genPattern(arguments)

    def writeSequence(seq: Seq[Transaction[Int, Int, Int]]): Unit = {
      val seqId = seq.head.sequenceId
      println((s"$seqId" +: seq.map(_.items.mkString(" "))).mkString(" -1 "))
    }

    @tailrec
    def writeNext(sequenceLeft: Int, supportingLeft: Int, id: Int = 1): Unit =
      if (supportingLeft > 0 || sequenceLeft > 0) {
        if (supportingLeft > sequenceLeft) {
          writeSequence(genSupportingSequence(id, pattern, arguments))
          writeNext(sequenceLeft, supportingLeft - 1, id + 1)
        } else {
          writeSequence(genRandomSequence(id, arguments))
          writeNext(sequenceLeft - 1, supportingLeft, id + 1)
        }
      }

    writeNext(arguments.sequenceCount - frequentSequenceCount, frequentSequenceCount)
  }

  def genSupportingSequence(id: Int, pattern: Pattern[Int], args: Arguments): Seq[Transaction[Int, Int, Int]] = {
    (pattern.elements flatMap { element =>
      if (Math.random() < 0.2) element :: genElement :: Nil
      else element :: Nil
    } map { element =>
      if (Math.random() < 0.1) enrichElement(element)
      else element
    } zipWithIndex) map { case (element, i) =>
      Transaction(id, i + 1, element.items)
    }
  }

  def genRandomSequence(id: Int, args: Arguments): Seq[Transaction[Int, Int, Int]] = {
    val length = (args.frequentSequenceElementCount * 1.2).toInt
    1 to length map { i =>
      Transaction(id, i, genElement.items)
    }
  }

  def genPattern(args: Arguments): Pattern[Int] = {
    Pattern(0 until args.frequentSequenceElementCount map { _ => genElement } toVector)
  }

  def genElement: Element[Int] = {
    val maxItemCount = 5.0
    val itemCount = Math.max(1, Math.floor(Math.random() * (maxItemCount + 0.5)).toInt)

    val itemValueMultiplier = 1000
    Element {
      1 to itemCount map { i =>
        (Math.random() * i * itemValueMultiplier).toInt
      } toSet
    }
  }

  def enrichElement(element: Element[Int]): Element[Int] =
    element.copy(element.items ++ genElement.items)
}
