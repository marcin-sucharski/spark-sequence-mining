package one.off_by.sequence.mining.gsp

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole


@State(Scope.Benchmark)
class HashTreeTest {

  import HashTreeTest.testParamsShortPattern

  @Param(Array("none", "withWindowSize", "withMinGap", "withMaxGap", "withAll"))
  var optionsMode: String = _

  @Setup
  def setup(): Unit = {
    options = HashTreeTest.options.optionsMap(optionsMode)
  }

  private var options: Option[HashTreeTest.options.OptionsType] = _

  @Benchmark
  def measureHashTreeFind(bh: Blackhole): Unit = {
    import testParamsShortPattern._
    hashTree.findPossiblePatterns(options, sequence).foreach(bh.consume)
  }
}

private object HashTreeTest {

  case class TestParams(patternSize: Int, transactionSize: Int) {
    val sequence: List[Transaction[Int, Int, Int]] =
      (1 to patternSize sliding transactionSize).zipWithIndex.toList map { case (seq, i) =>
        Transaction(1, i, seq.toSet)
      }

    val candidatePatterns: Seq[Pattern[Int]] =
      (for {
        x <- 1 to patternSize * 3
        y <- 1 to patternSize * 3
        if x != y
      } yield Pattern(Vector(Element(x), Element(y))) :: Pattern(Vector(Element(x, y))) :: Nil).flatten.distinct

    val hashTree: HashTree[Int, Int, Int, Int] =
      ((HashTreeLeaf[Int, Int, Int, Int](): HashTree[Int, Int, Int, Int]) /: candidatePatterns) (_ add _)
  }

  val testParamsShortPattern = TestParams(200, 4)

  object options {
    type OptionsType = GSPOptions[Int, Int]
    val typeSupport: GSPTypeSupport[Int, Int] = GSPTypeSupport[Int, Int]((a, b) => b - a, _ - _, _ + _)

    val none: Option[OptionsType] = None
    val withWindowSize: Option[OptionsType] = Some(GSPOptions[Int, Int](typeSupport, windowSize = Some(15)))
    val withMinGap: Option[OptionsType] = Some(GSPOptions[Int, Int](typeSupport, minGap = Some(20)))
    val withMaxGap: Option[OptionsType] = Some(GSPOptions[Int, Int](typeSupport, maxGap = Some(40)))
    val withAll: Option[OptionsType] = Some(GSPOptions[Int, Int](
      typeSupport,
      windowSize = Some(10),
      minGap = Some(20),
      maxGap = Some(40)))

    val optionsMap = Map(
      "none" -> none,
      "withWindowSize" -> withWindowSize,
      "withMinGap" -> withMinGap,
      "withMaxGap" -> withMaxGap,
      "withAll" -> withAll)
  }
}
