package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.utils.LoggingUtils
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}

import scala.language.postfixOps
import scala.reflect.ClassTag

case class GSPOptions[
@specialized(Int, Long, Float, Double) TimeType,
@specialized(Int, Long, Float, Double) DurationType](
  typeSupport: GSPTypeSupport[TimeType, DurationType],
  windowSize: Option[DurationType] = None,
  minGap: Option[DurationType] = None,
  maxGap: Option[DurationType] = None
)

case class GSPTypeSupport[
@specialized(Int, Long, Float, Double) TimeType,
@specialized(Int, Long, Float, Double) DurationType](
  timeDistance: (TimeType, TimeType) => DurationType,
  timeSubtract: (TimeType, DurationType) => TimeType,
  timeAdd: (TimeType, DurationType) => TimeType
)(implicit val durationOrdering: Ordering[DurationType])

class GSP[
@specialized(Int, Long, Float, Double) ItemType: ClassTag,
@specialized(Int, Long, Float, Double) DurationType,
@specialized(Int, Long, Float, Double) TimeType,
SequenceId: ClassTag](
  sc: SparkContext
)(
  implicit
  timeOrdering: Ordering[TimeType],
  itemOrdering: Ordering[ItemType]
) extends LoggingUtils {

  import Domain.{Percent, SupportCount}

  type TransactionType = Transaction[ItemType, TimeType, SequenceId]

  private[gsp] val partitioner: Partitioner =
    new HashPartitioner(sc.getConf.getInt("spark.default.parallelism", 2))

  private val patternJoiner = new PatternJoiner[ItemType](partitioner)

  def execute(
    transactions: RDD[TransactionType],
    minSupport: Percent,
    minItemsInPattern: Long = 1L,
    maybeOptions: Option[GSPOptions[TimeType, DurationType]] = None
  ): RDD[(Pattern[ItemType], SupportCount)] = {
    def maybeFilterOut(itemsCount: Long, result: RDD[(Pattern[ItemType], SupportCount)]) =
      if (itemsCount >= minItemsInPattern) result
      else sc.parallelize(List.empty[(Pattern[ItemType], SupportCount)])

    val sequences: RDD[(SequenceId, TransactionType)] = transactions
      .map(t => (t.sequenceId, t))
      .partitionBy(partitioner)
      .cache()

    val sequenceCount = sequences.keys.distinct.count()
    val minSupportCount = (sequenceCount * minSupport).toInt
    logger.info(s"Total sequence count is $sequenceCount and minimum support count is $minSupportCount.")

    val patternMatcher = createPatternMatcher(maybeOptions, sequences, minSupportCount)

    val initialPatterns = prepareInitialPatterns(sequences, minSupportCount)
      .persist(StorageLevels.MEMORY_AND_DISK_SER)

    initialPatterns.take(1)
    sequences.unpersist()

    logger.info(s"Got ${initialPatterns.count()} initial patterns.")
    val result = Stream.iterate(State(initialPatterns, initialPatterns, 1L)) { case State(acc, prev, prevLength) =>
      val newLength = prevLength + 1
      logger.info(s"Merging patterns of size $prevLength into $newLength.")
      val candidates = patternJoiner.generateCandidates(prev.map(_._1))
      logger.trace(s"Candidates: ${candidates.map(_.toString).toPrettyList}")
      val candidateCount = sc.longAccumulator
      val phaseResult = patternMatcher.filter(candidates, Some(candidateCount))
        .persist(StorageLevels.MEMORY_AND_DISK_SER)
      logger.info(s"Got ${phaseResult.count()} patterns as result.")
      logger.info(s"There was approximately ${candidateCount.value} candidates.")
      State(maybeFilterOut(newLength, acc.union(phaseResult)), phaseResult, newLength)
    } takeWhile (!_.lastPattern.isEmpty()) lastOption

    result.map(_.result).getOrElse(sc.parallelize(Nil))
  }

  // Creation of PatternMatcher is extracted into separated method thus it can be specialized
  private def createPatternMatcher(
    maybeOptions: Option[GSPOptions[TimeType, DurationType]],
    sequences: RDD[(SequenceId, TransactionType)],
    minSupportCount: SupportCount): PatternMatcher[ItemType, TimeType, DurationType, SequenceId] =
    new PatternMatcher[ItemType, TimeType, DurationType, SequenceId](
      sc, partitioner, sequences, maybeOptions, minSupportCount)

  private case class State(
    result: RDD[(Pattern[ItemType], SupportCount)],
    lastPattern: RDD[(Pattern[ItemType], SupportCount)],
    itemsCount: Long
  )

  private[gsp] def prepareInitialPatterns(
    sequences: RDD[(SequenceId, TransactionType)],
    minSupportCount: SupportCount
  ): RDD[(Pattern[ItemType], SupportCount)] = {
    assume(sequences.partitioner contains partitioner)
    sequences
      .flatMapValues(_.items.map(Element(_)))
      .mapPartitions(_.toSet.toIterator, preservesPartitioning = true)
      .values
      .map(element => (Pattern(Vector(element)), 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minSupportCount)
  }
}
