package one.off_by.sequence.mining.gsp

import spray.json._

object StatisticsSerializer {
  def serializerToJson(statisticsReader: StatisticsReader, totalTimeMillis: Long): String =
    Statistics(
      transactionCount = statisticsReader.transactionCount,
      sequenceCount = statisticsReader.sequenceCount,
      totalTimeMillis = totalTimeMillis,
      phases = statisticsReader.phases.toList map { phase =>
        PhaseResult(
          index = phase.index,
          resultPatternsCount = phase.resultPatternsCount,
          approximateCandidateCount = phase.approximateCandidateCount
        )
      }
    ).toJson.prettyPrint

  private case class Statistics(
    transactionCount: Int,
    sequenceCount: Int,
    totalTimeMillis: Long,
    phases: List[PhaseResult]
  )

  private case class PhaseResult(
    index: Int,
    resultPatternsCount: Int,
    approximateCandidateCount: Long
  )

  import DefaultJsonProtocol._

  private implicit lazy val phaseResultFormat: JsonFormat[PhaseResult] = jsonFormat3(PhaseResult)
  private implicit lazy val statisticsFormat: JsonFormat[Statistics] = jsonFormat4(Statistics)
}
