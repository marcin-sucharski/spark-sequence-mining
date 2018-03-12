package one.off_by.sequence.mining.gsp

import scala.collection.mutable

trait StatisticsGatherer {
  def saveTransactionCount(transactionCount: Int): Unit

  def saveSequenceCount(sequenceCount: Int): Unit

  def saveNextPhaseStatistics(
    resultPatternsCount: Int,
    approximateCandidateCount: Long
  ): Unit
}

trait StatisticsReader {
  def transactionCount: Int

  def sequenceCount: Int

  def phases: Iterable[PhaseResult]
}

case class PhaseResult(
  index: Int,
  resultPatternsCount: Int,
  approximateCandidateCount: Long
)

class SimpleStatisticsGatherer private(
  var _transactionCount: Int = -1,
  var _sequenceCount: Int = -1,
  val _phases: mutable.ArrayBuffer[PhaseResult] = mutable.ArrayBuffer()
) extends StatisticsGatherer with StatisticsReader {

  override def saveTransactionCount(transactionCount: Int): Unit = this._transactionCount = transactionCount

  override def saveSequenceCount(sequenceCount: Int): Unit = this._sequenceCount = sequenceCount

  override def saveNextPhaseStatistics(resultPatternsCount: Int, approximateCandidateCount: Long): Unit =
    _phases.append(PhaseResult(
      index = _phases.size + 1,
      resultPatternsCount = resultPatternsCount,
      approximateCandidateCount = approximateCandidateCount
    ))

  override def transactionCount: Int = _transactionCount

  override def sequenceCount: Int = _sequenceCount

  override def phases: Iterable[PhaseResult] = _phases.toList
}

object SimpleStatisticsGatherer {
  def apply(): SimpleStatisticsGatherer = new SimpleStatisticsGatherer()
}
