package one.off_by.sequence.mining.gsp

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.reflect.ClassTag

@specialized
case class GSPOptions[TimeType, DurationType](
  timeDistance: (TimeType, TimeType) => DurationType,
  windowSize: Option[DurationType] = None,
  minGap: Option[DurationType] = None,
  maxGap: Option[DurationType] = None
) {
  assert(windowSize.isDefined || minGap.isDefined || maxGap.isDefined)
}

@specialized
class GSP[ItemType: ClassTag, DurationType, TimeType, SequenceId: ClassTag](
  sc: SparkContext
)(
  implicit timeOrdering: Ordering[TimeType]
) {

  import Domain.{Percent, Support, SupportCount}

  type TaxonomyType = Taxonomy[ItemType]
  type TransactionType = Transaction[ItemType, TimeType, SequenceId]

  assert(sc.getConf.contains("spark.default.parallelism"))
  private[gsp] val partitioner: Partitioner =
    new HashPartitioner(sc.getConf.getInt("spark.default.parallelism", 2) * 4)

  def execute(
    transactions: RDD[TransactionType],
    minSupport: Percent,
    maybeTaxonomies: Option[RDD[TaxonomyType]] = None,
    maybeOptions: Option[GSPOptions[TimeType, DurationType]] = None
  ): RDD[(Pattern[ItemType], Support)] = {
    val sequences: RDD[(SequenceId, TransactionType)] = transactions
      .map(t => (t.sequenceId, t))
      .partitionBy(partitioner)
      .cache()
    val sequenceCount = sequences.keys.count()
    val minSupportCount = (sequenceCount * (minSupport * 0.01)).toLong

    val initialPatterns = prepareInitialPatterns(sequences, minSupportCount)

    ???
  }

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

  private[gsp] def generateJoinCandidates(in: RDD[Pattern[ItemType]]): GSP.JoinCandidatesResult[ItemType] =
    ???


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
