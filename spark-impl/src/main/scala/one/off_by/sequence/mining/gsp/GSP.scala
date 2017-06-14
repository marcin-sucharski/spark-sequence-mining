package one.off_by.sequence.mining.gsp

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

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
  type Percent = Double
  type Support = Percent

  type TaxonomyType = Taxonomy[ItemType]
  type TransactionType = Transaction[ItemType, TimeType, SequenceId]

  def execute(
    transactions: RDD[TransactionType],
    minSupport: Percent,
    maybeTaxonomies: Option[RDD[TaxonomyType]] = None,
    maybeOptions: Option[GSPOptions[TimeType, DurationType]] = None
  ): RDD[(Support, List[ItemType])] = {
    val sequenceCount = transactions.map(_.sequenceId).distinct().count()
    val minSupportCount = (sequenceCount * (minSupport * 0.01)).toLong

    val sequences = prepareInitialSequences(transactions, minSupportCount)

    ???
  }

  private[gsp] def prepareInitialSequences(transactions: RDD[TransactionType], minSupportCount: Long) =
    transactions
      .map(t => (t.sequenceId, t))
      .partitionBy(new HashPartitioner(sc.defaultMinPartitions))
      .flatMapValues(_.items.subsets.filter(_.nonEmpty))
      .distinct()
      .map(p => (p._2, 1L))
      .reduceByKey(_ + _)
      .filter(_._2 >= minSupportCount)
      .map(_._1 :: Nil)

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
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("GSP")
      .set("spark.driver.host", "localhost")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)


  }
}
