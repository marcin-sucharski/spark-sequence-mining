package one.off_by.sequence.mining.gsp

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.util.Random

@State(Scope.Benchmark)
class CollectionDistinctUtilsTest {
  @Param(Array("noDuplicates", "minorDuplicates", "manyDuplicates"))
  var inputMode: String = _

  @Setup
  def setup(): Unit = {
    def fromInt(i: Int): TestItem = TestItem(i, 1000 - i)

    input = (inputMode match {
      case "noDuplicates" =>
        1 to 1000

      case "minorDuplicates" =>
        (1 to 900) ++ Random.shuffle(1 to 900).take(100)

      case "manyDuplicates" =>
        (1 to 500) ++ (1 to 500)

      case _ =>
        sys.error("incorrect inputMode")
    }) map fromInt

    input = Random.shuffle(input)
  }

  var input: Iterable[TestItem] = _

  def inputIter: Iterator[TestItem] = input.iterator

  @Benchmark
  def measureDistinctCalculationCustom(bh: Blackhole): Any = {
    import one.off_by.sequence.mining.gsp.utils.CollectionDistinctUtils._

    inputIter.distinct.foreach(bh.consume(_: Object))
  }

  @Benchmark
  def measureDistinctCalculationStdlibSet(bh: Blackhole): Any = {
    inputIter.toSet.toIterator.foreach(bh.consume(_: Object))
  }

  @Benchmark
  def measureDistinctCalculationStdlibVector(bh: Blackhole): Any = {
    inputIter.toVector.distinct.toIterator.foreach(bh.consume(_: Object))
  }
}

case class TestItem(a: Int, b: Int)
