package one.off_by.sequence.mining.gsp

import scala.util.{Failure, Success, Try}

final case class GSPArguments(
  inputFile: String,
  outputFile: String,
  minSupport: Double,
  minItemsInPattern: Int,
  windowSize: Option[Int],
  minGap: Option[Int],
  maxGap: Option[Int]
) {
  def toOptions: Option[GSPOptions[Int, Int]] = {
    val intTypeSupport: GSPTypeSupport[Int, Int] = GSPTypeSupport[Int, Int](
      (a, b) => b - a,
      (a, b) => a - b,
      (a, b) => a + b
    )

    Some(GSPOptions(intTypeSupport, windowSize, minGap, maxGap))
      .filter(_ => windowSize.isDefined || minGap.isDefined || maxGap.isDefined)
  }
}

final case class ArgumentParserException(reason: String) extends Exception(s"Failed to parse arguments: $reason")

object ArgumentParser {
  def parseArguments(args: Array[String]): GSPArguments =
    validate(parseDown(args.toList))

  private[this] def validate(builder: GSPArgumentsBuilder): GSPArguments = {
    val result = GSPArguments(
      inputFile = builder.inputFile.getOrElse(throw ArgumentParserException("missing input-file")),
      outputFile = builder.outputFile.getOrElse(throw ArgumentParserException("missing output-file")),
      minSupport = builder.minSupport.getOrElse(0.5),
      minItemsInPattern = builder.minItemsInPattern.getOrElse(1),
      windowSize = builder.windowSize,
      minGap = builder.minGap,
      maxGap = builder.maxGap
    )

    def check(message: String)(condition: => Boolean): Unit =
      if (!condition) throw ArgumentParserException(s"check not passed: $message")

    check("min-support is greater than zero")(result.minSupport > 0.0)
    check("min-items-in-pattern is greater than zero")(result.minItemsInPattern > 0)
    result.windowSize foreach { windowSize =>
      check("window-size is at least zero")(windowSize >= 0)
    }
    result.minGap foreach { minGap =>
      check("min-gap is at least zero")(minGap >= 0)
    }
    result.maxGap foreach { maxGap =>
      check("max-gap is at least zero")(maxGap >= 0)
    }

    result
  }

  private[this] def parseDown(args: List[String]): GSPArgumentsBuilder = args match {
    case "--in" :: path :: rest =>
      GSPArgumentsBuilder(inputFile = Some(path))
        .overrideWith(parseDown(rest))

    case "--out" :: path :: rest =>
      GSPArgumentsBuilder(outputFile = Some(path))
        .overrideWith(parseDown(rest))

    case "--min-support" :: value :: rest =>
      GSPArgumentsBuilder(minSupport = Some(wrapException("min-support", value.toDouble)))
        .overrideWith(parseDown(rest))

    case "--min-items-in-pattern" :: value :: rest =>
      GSPArgumentsBuilder(minItemsInPattern = Some(wrapException("min-items-in-pattern", value.toInt)))
        .overrideWith(parseDown(rest))

    case "--window-size" :: value :: rest =>
      GSPArgumentsBuilder(windowSize = Some(wrapException("window-size", value.toInt)))
        .overrideWith(parseDown(rest))

    case "--min-gap" :: value :: rest =>
      GSPArgumentsBuilder(minGap = Some(wrapException("min-gap", value.toInt)))
        .overrideWith(parseDown(rest))

    case "--max-gap" :: value :: rest =>
      GSPArgumentsBuilder(maxGap = Some(wrapException("max-gap", value.toInt)))
        .overrideWith(parseDown(rest))

    case Nil =>
      GSPArgumentsBuilder()

    case invalid :: _ =>
      throw ArgumentParserException(s"invalid argument: $invalid")
  }

  private[this] def wrapException[T](argument: String, inner: => T): T =
    Try(inner) match {
      case Success(value) => value
      case Failure(e)     => throw ArgumentParserException(s"$argument due to ${e.getLocalizedMessage}")
    }

  private[this] final case class GSPArgumentsBuilder(
    inputFile: Option[String] = None,
    outputFile: Option[String] = None,
    minSupport: Option[Double] = None,
    minItemsInPattern: Option[Int] = None,
    windowSize: Option[Int] = None,
    minGap: Option[Int] = None,
    maxGap: Option[Int] = None
  ) {
    def overrideWith(other: GSPArgumentsBuilder): GSPArgumentsBuilder =
      GSPArgumentsBuilder(
        inputFile = other.inputFile.orElse(inputFile),
        outputFile = other.outputFile.orElse(outputFile),
        minSupport = other.minSupport.orElse(minSupport),
        minItemsInPattern = other.minItemsInPattern.orElse(minItemsInPattern),
        windowSize = other.windowSize.orElse(windowSize),
        minGap = other.minGap.orElse(minGap),
        maxGap = other.maxGap.orElse(maxGap)
      )
  }

}
