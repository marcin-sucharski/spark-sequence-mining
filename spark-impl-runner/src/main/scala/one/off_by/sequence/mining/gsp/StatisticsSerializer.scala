package one.off_by.sequence.mining.gsp

import spray.json._

object StatisticsSerializer {
  def serializerToJson(statisticsReader: StatisticsReader, gspArguments: GSPArguments, totalTimeMillis: Long): String =
    Statistics(
      transactionCount = statisticsReader.transactionCount,
      sequenceCount = statisticsReader.sequenceCount,
      totalTimeMillis = totalTimeMillis,
      arguments = Arguments(
        input = gspArguments.inputFile,
        output = gspArguments.outputFile,
        minSupport = gspArguments.minSupport,
        minItemsInPattern = gspArguments.minItemsInPattern,
        windowSize = gspArguments.windowSize,
        minGap = gspArguments.minGap,
        maxGap = gspArguments.maxGap
      ),
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
    arguments: Arguments,
    phases: List[PhaseResult]
  )

  private case class Arguments(
    input: String,
    output: String,
    minSupport: Double,
    minItemsInPattern: Int,
    windowSize: Option[Int],
    minGap: Option[Int],
    maxGap: Option[Int]
  )

  private case class PhaseResult(
    index: Int,
    resultPatternsCount: Int,
    approximateCandidateCount: Long
  )

  import DefaultJsonProtocol._

  private implicit lazy val phaseResultFormat: JsonFormat[PhaseResult] = jsonFormat3(PhaseResult)
  private implicit lazy val argumentsFormat: JsonFormat[Arguments] = jsonFormat7(Arguments)
  private implicit lazy val statisticsFormat: JsonFormat[Statistics] = jsonFormat5(Statistics)
}
