package one.off_by.sequence.mining

package object gsp {
  object Helper {

    @specialized
    implicit class TimeSupport[TimeType, DurationType](time: TimeType)(
      implicit typeSupport: GSPTypeSupport[TimeType, DurationType]) {

      /**
        * Computes the duration between two time points.
        *
        * @note Initially this method was named `-` (minus) however after specialization it conflicts
        * with `-` (minus) for duration.
        */
      def distanceTo(other: TimeType): DurationType = typeSupport.timeDistance(time, other)

      def -(duration: DurationType): TimeType = typeSupport.timeSubtract(time, duration)

      def +(duration: DurationType): TimeType = typeSupport.timeAdd(time, duration)
    }

  }
}
