package one.off_by.sequence.mining.gsp

import org.scalatest.{FreeSpec, Matchers}

class TimeSupportSpec extends FreeSpec
  with Matchers {

  "TimeSupport should" - {
    "implement user friendly interface for common time methods" in {
      import one.off_by.sequence.mining.gsp.Helper.TimeSupport

      final case class Time(value: Int)
      final case class Duration(value: Int)

      implicit val durationOrdering: Ordering[Duration] = Ordering.Int.on[Duration](_.value)
      implicit val typeSupport: GSPTypeSupport[Time, Duration] = GSPTypeSupport[Time, Duration](
        timeDistance = (a, b) => Duration(b.value - a.value),
        timeSubtract = (a, b) => Time(a.value - b.value),
        timeAdd = (a, b) => Time(a.value + b.value)
      )

      val first = Time(5)
      val second = Time(10)
      val duration = Duration(3)

      first distanceTo second shouldBe Duration(5)
      second - duration shouldBe Time(7)
      second + duration shouldBe Time(13)
    }
  }
}
