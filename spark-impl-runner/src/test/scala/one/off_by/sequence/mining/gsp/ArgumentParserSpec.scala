package one.off_by.sequence.mining.gsp

import org.scalatest.{Matchers, OptionValues, WordSpec}

class ArgumentParserSpec
  extends WordSpec
  with Matchers
  with OptionValues {

  "ArgumentParser" should {
    "provide 'parseArguments' method" which {
      "works for simple argument set" in {
        val args = Array(
          "--in", "in_file",
          "--out", "out_file"
        )

        val result = ArgumentParser.parseArguments(args)

        result.inputFile shouldBe "in_file"
        result.outputFile shouldBe "out_file"
      }

      "parses all arguments" in {
        val args = Array(
          "--in", "in_file",
          "--out", "out_file",
          "--min-support", "0.2",
          "--min-items-in-pattern", "5",
          "--window-size", "4",
          "--min-gap", "2",
          "--max-gap", "6"
        )

        val result = ArgumentParser.parseArguments(args)

        result shouldBe GSPArguments(
          inputFile = "in_file",
          outputFile = "out_file",
          minSupport = 0.2,
          minItemsInPattern = 5,
          windowSize = Some(4),
          minGap = Some(2),
          maxGap = Some(6)
        )
      }

      "if argument is repeated then last value is taken" in {
        val args = Array(
          "--in", "in_file",
          "--out", "out_file",
          "--min-support", "0.3",
          "--in", "new_in"
        )

        val result = ArgumentParser.parseArguments(args)

        result.inputFile shouldBe "new_in"
      }

      "throws an exception if min-support is incorrect" in {
        val args = Array(
          "--in", "in_file",
          "--out", "out_file",
          "--min-support", "abc"
        )

        an[Exception] should be thrownBy ArgumentParser.parseArguments(args)
      }

      "throws an exception if min-items-in-pattern is incorrect" in {
        val args = Array(
          "--in", "in_file",
          "--out", "out_file",
          "--min-items-in-pattern", "0.1"
        )

        an[Exception] should be thrownBy ArgumentParser.parseArguments(args)
      }

      "throws an exception if window-size is incorrect" in {
        val args = Array(
          "--in", "in_file",
          "--out", "out_file",
          "--window-size", "aaa"
        )

        an[Exception] should be thrownBy ArgumentParser.parseArguments(args)
      }

      "throws an exception if min-gap is incorrect" in {
        val args = Array(
          "--in", "in_file",
          "--out", "out_file",
          "--min-gap", "-2"
        )

        an[Exception] should be thrownBy ArgumentParser.parseArguments(args)
      }

      "throws an exception if max-gap is incorrect" in {
        val args = Array(
          "--in", "in_file",
          "--out", "out_file",
          "--max-gap", "xyz"
        )

        an[Exception] should be thrownBy ArgumentParser.parseArguments(args)
      }

      "throws an exception if incorrect argument is passed" in {
        val args = Array(
          "--in", "in_file",
          "--out", "out_file",
          "--incorrect-arg", "value"
        )

        an[Exception] should be thrownBy ArgumentParser.parseArguments(args)
      }

      "throws an exception if arguments are in incorrect format" in {
        val args = Array(
          "--in", "in_file",
          "--out", "out_file",
          "not_an_arg"
        )

        an[Exception] should be thrownBy ArgumentParser.parseArguments(args)
      }
    }
  }

  "GSPArguments" should {
    "provide 'toOptions' method" which {
      "generates correct GSPOptions" in {
        val args = GSPArguments(
          inputFile = "",
          outputFile = "",
          minSupport = 0.2,
          minItemsInPattern = 1,
          windowSize = Some(2),
          minGap = Some(1),
          maxGap = Some(2)
        )

        val result = args.toOptions.value

        result.windowSize shouldBe args.windowSize
        result.maxGap shouldBe args.maxGap
        result.minGap shouldBe args.minGap
      }

      "returns None if no options are defined" in {
        val args = GSPArguments(
          inputFile = "",
          outputFile = "",
          minSupport = 0.2,
          minItemsInPattern = 1,
          windowSize = None,
          minGap = None,
          maxGap = None
        )

        args.toOptions shouldNot be (defined)
      }

      "returns correct type support" in {
        val args = GSPArguments(
          inputFile = "",
          outputFile = "",
          minSupport = 0.2,
          minItemsInPattern = 1,
          windowSize = Some(2),
          minGap = Some(1),
          maxGap = Some(2)
        )

        val result = args.toOptions.value.typeSupport

        result.timeAdd(1, 3) shouldBe 4
        result.timeDistance(5, 7) shouldBe 2
        result.timeSubtract(5, 2) shouldBe 3
        result.durationOrdering.lt(1, 2) shouldBe true
      }
    }
  }
}
