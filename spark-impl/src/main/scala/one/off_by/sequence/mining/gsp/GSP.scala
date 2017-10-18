package one.off_by.sequence.mining.gsp

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.language.postfixOps
import scala.reflect.ClassTag

@specialized
case class GSPOptions[TimeType, DurationType](
  typeSupport: GSPTypeSupport[TimeType, DurationType],
  windowSize: Option[DurationType] = None,
  minGap: Option[DurationType] = None,
  maxGap: Option[DurationType] = None
)

@specialized
case class GSPTypeSupport[TimeType, DurationType](
  timeDistance: (TimeType, TimeType) => DurationType,
  timeSubtract: (TimeType, DurationType) => TimeType,
  timeAdd: (TimeType, DurationType) => TimeType
)(implicit val durationOrdering: Ordering[DurationType])

@specialized
class GSP[ItemType: ClassTag, DurationType, TimeType, SequenceId: ClassTag](
  sc: SparkContext,
  patternHasher: PatternHasher[ItemType] = new DefaultPatternHasher[ItemType]()
)(
  implicit timeOrdering: Ordering[TimeType]
) extends StrictLogging {

  import Domain.{Percent, Support, SupportCount}

  type TaxonomyType = Taxonomy[ItemType]
  type TransactionType = Transaction[ItemType, TimeType, SequenceId]

  assert(sc.getConf.contains("spark.default.parallelism"))
  private[gsp] val partitioner: Partitioner =
    new HashPartitioner(sc.getConf.getInt("spark.default.parallelism", 2) * 4)

  private val broadcastHasher = sc.broadcast(patternHasher)
  private val patternJoiner = new PatternJoiner[ItemType](broadcastHasher, partitioner)

  def execute(
    transactions: RDD[TransactionType],
    minSupport: Percent,
    minItemsInPattern: Long = 1L,
    maybeTaxonomies: Option[RDD[TaxonomyType]] = None,
    maybeOptions: Option[GSPOptions[TimeType, DurationType]] = None
  ): RDD[(Pattern[ItemType], Support)] = {
    def maybeFilterOut(itemsCount: Long, result: RDD[(Pattern[ItemType], SupportCount)]) =
      if (itemsCount >= minItemsInPattern) result
      else sc.parallelize(List.empty[(Pattern[ItemType], SupportCount)])

    val sequences: RDD[(SequenceId, TransactionType)] = transactions
      .map(t => (t.sequenceId, t))
      .partitionBy(partitioner)
      .cache()

    val sequenceCount = sequences.keys.distinct.count()
    val minSupportCount = (sequenceCount * minSupport).toLong
    logger.info(s"Total sequence count is $sequenceCount and minimum support count is $minSupportCount")

    val patternMatcher = new PatternMatcher(partitioner, sequences, maybeOptions, minSupportCount)

    val initialPatterns = prepareInitialPatterns(sequences, minSupportCount).cache()
    Stream.iterate(State(initialPatterns, initialPatterns, 1L)) { case State(acc, prev, prevLength) =>
      val newLength = prevLength + 1
      logger.info(s"Merging patterns of size $prevLength into $newLength")
      val candidates = patternJoiner.generateCandidates(prev.map(_._1))
      val phaseResult = patternMatcher.filter(candidates).cache()
      State(maybeFilterOut(newLength, acc.union(phaseResult)), phaseResult, newLength)
    } takeWhile (!_.lastPattern.isEmpty()) map (_.result.mapValues(_.toDouble / sequenceCount.toDouble)) last
  }

  private case class State(
    result: RDD[(Pattern[ItemType], SupportCount)],
    lastPattern: RDD[(Pattern[ItemType], SupportCount)],
    itemsCount: Long
  )

  private[gsp] def prepareInitialPatterns(
    sequences: RDD[(SequenceId, TransactionType)],
    minSupportCount: Long
  ): RDD[(Pattern[ItemType], SupportCount)] = {
    require(sequences.partitioner contains partitioner)
    sequences
      .flatMapValues(_.items.map(Element(_)))
      .mapPartitions(_.toSet.toIterator, preservesPartitioning = true)
      .values
      .map(element => (Pattern(Vector(element)), 1L))
      .reduceByKey(_ + _)
      .filter(_._2 >= minSupportCount)
  }

  private[gsp] def prepareTaxonomies(
    taxonomies: RDD[TaxonomyType]
  ): RDD[(ItemType, List[ItemType])] = {
    val children = taxonomies
      .flatMap(_.descendants)
      .map(d => (d, d))
    val joinTarget = taxonomies
      .map(t => (t.ancestor, t))
      .partitionBy(new HashPartitioner(sc.defaultMinPartitions))
      .cache()
    val roots = joinTarget
      .leftOuterJoin(children)
      .filter(t => t._2._2.isEmpty)
      .map(t => t._2._1)
      .flatMap(t => t.descendants.map(d => (d, d :: t.ancestor :: Nil)))

    def expand(temp: RDD[(ItemType, List[ItemType])]): RDD[List[ItemType]] = {
      val expanded = joinTarget.rightOuterJoin(temp).cache()
      val expansionCount = expanded.filter(_._2._1.isDefined).count()
      val result = if (expansionCount > 0) {
        expand(expanded.flatMap {
          case (_, (Some(target), rest)) =>
            target.descendants.map(d => (d, d :: rest))

          case (last, (None, rest)) =>
            (last, rest) :: Nil
        })
      } else temp.map(_._2)
      expanded.unpersist()
      result
    }

    joinTarget.unpersist()
    expand(roots).map(items => (items.head, items.reverse))
  }
}

object GSP {

  import Domain._

  private[gsp] case class JoinCandidatesResult[ItemType](
    prefixHash: RDD[(Hash[ItemType], Pattern[Element[ItemType]])],
    suffixHash: RDD[(Hash[ItemType], Pattern[Element[ItemType]])]
  )

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("GSP")
      .set("spark.driver.host", "localhost")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)


  }
}
