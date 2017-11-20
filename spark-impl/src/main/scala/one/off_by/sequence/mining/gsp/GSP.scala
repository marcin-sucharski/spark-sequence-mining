package one.off_by.sequence.mining.gsp

import grizzled.slf4j.Logging
import one.off_by.sequence.mining.gsp.Domain.Support
import one.off_by.sequence.mining.gsp.PatternJoiner.{JoinItemExistingElement, JoinItemNewElement, PrefixResult, SuffixResult}
import one.off_by.sequence.mining.gsp.PatternMatcher.SearchableSequence
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NonFatal

case class GSPOptions[TimeType, DurationType](
  typeSupport: GSPTypeSupport[TimeType, DurationType],
  windowSize: Option[DurationType] = None,
  minGap: Option[DurationType] = None,
  maxGap: Option[DurationType] = None
)

case class GSPTypeSupport[TimeType, DurationType](
  timeDistance: (TimeType, TimeType) => DurationType,
  timeSubtract: (TimeType, DurationType) => TimeType,
  timeAdd: (TimeType, DurationType) => TimeType
)(implicit val durationOrdering: Ordering[DurationType])

class GSP[ItemType: ClassTag, DurationType, TimeType, SequenceId: ClassTag](
  sc: SparkContext
)(
  implicit
  timeOrdering: Ordering[TimeType],
  itemOrdering: Ordering[ItemType]
) extends Logging {

  import Domain.{Percent, Support, SupportCount}

  type TaxonomyType = Taxonomy[ItemType]
  type TransactionType = Transaction[ItemType, TimeType, SequenceId]

  private[gsp] val partitioner: Partitioner =
    new HashPartitioner(sc.getConf.getInt("spark.default.parallelism", 2))

  private val patternJoiner = new PatternJoiner[ItemType](partitioner)

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
      .persist(StorageLevel.MEMORY_ONLY_SER)

    val sequenceCount = sequences.keys.distinct.count()
    val minSupportCount = (sequenceCount * minSupport).toLong
    logger.info(s"Total sequence count is $sequenceCount and minimum support count is $minSupportCount.")

    val patternMatcher = new PatternMatcher(sc, partitioner, sequences, maybeOptions, minSupportCount)

    val initialPatterns = prepareInitialPatterns(sequences, minSupportCount)
      .persist(StorageLevels.MEMORY_AND_DISK_SER)
    logger.info(s"Got ${initialPatterns.count()} initial patterns.")
    val result = Stream.iterate(State(initialPatterns, initialPatterns, 1L)) { case State(acc, prev, prevLength) =>
      val newLength = prevLength + 1
      logger.info(s"Merging patterns of size $prevLength into $newLength.")
      val candidates = patternJoiner.generateCandidates(prev.map(_._1))
      val phaseResult = patternMatcher.filter(candidates)
        .persist(StorageLevels.MEMORY_AND_DISK_SER)
      logger.info(s"Got ${phaseResult.count()} patterns as result.")
      State(maybeFilterOut(newLength, acc.union(phaseResult)), phaseResult, newLength)
    } takeWhile (!_.lastPattern.isEmpty()) map (_.result.mapValues(_.toDouble / sequenceCount.toDouble)) lastOption

    result.getOrElse(sc.parallelize(Nil))
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
    assume(sequences.partitioner contains partitioner)
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
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("GSP")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(
        classOf[Transaction[_, _, _]],
        classOf[Taxonomy[_]],
        classOf[Element[_]],
        classOf[Pattern[_]],
        classOf[Array[Pattern[_]]],
        classOf[JoinItemNewElement[_]],
        classOf[JoinItemExistingElement[_]],
        classOf[PrefixResult[_]],
        classOf[SuffixResult[_]],
        classOf[SearchableSequence[_, _, _]],
        classOf[HashTree[_, _, _, _]],
        classOf[HashTreeLeaf[_, _, _, _]],
        classOf[HashTreeNode[_, _, _, _]]
      ))
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val inputFile = getArg(args)("in")
    val outputFile = getArg(args)("out")
    val minSupport = findArg(args)("min-support").map(_.toDouble).getOrElse(0.5)
    val minItemsInPattern = findArg(args)("min-items-in-pattern").map(_.toLong).getOrElse(1L)
    val windowSize = findArg(args)("window-size").map(_.toLong)
    val minGap = findArg(args)("min-gap").map(_.toLong)
    val maxGap = findArg(args)("max-gap").map(_.toLong)

    val gsp = new GSP[Long, Long, Long, Long](sc)
    val maybeOptions = Some(GSPOptions(longTypeSupport, windowSize, minGap, maxGap))
      .filter(_ => windowSize.isDefined || minGap.isDefined || maxGap.isDefined)

    val input = sc.textFile(inputFile, gsp.partitioner.numPartitions)
      .filter(_.trim.nonEmpty)
      .flatMap(parseLineIntoTransaction)

    gsp.execute(input, minSupport, minItemsInPattern, None, maybeOptions)
      .map(mapResultToString)
      .saveAsTextFile(outputFile)

    Console.println("Press any key to finish")
    Console.in.readLine()
  }

  def parseLineIntoTransaction(line: String): Seq[Transaction[Long, Long, Long]] =
    (try line.split("-1").map(_.split(" ").map(_.trim).filter(_.nonEmpty).map(_.toLong).toList).toList catch {
      case NonFatal(e) =>
        sys.error(s"malformed line due to ${e.getLocalizedMessage}: $line")
    }) match {
      case (sequenceId :: Nil) :: itemsets =>
        itemsets.filter(_.nonEmpty).zipWithIndex map { case (items, index) =>
          Transaction[Long, Long, Long](sequenceId, index + 1, items.toSet)
        }

      case _ =>
        sys.error(s"malformed line: $line")
    }

  def mapResultToString(result: (Pattern[Long], Support)): String =
    s"${result._2 * 100.0} # ${result._1}"

  val longTypeSupport: GSPTypeSupport[Long, Long] = GSPTypeSupport[Long, Long](
    (a, b) => b - a,
    (a, b) => a - b,
    (a, b) => a + b)

  private def findArg(args: Seq[String])(name: String): Option[String] = {
    val fullName = s"--$name"
    args.dropWhile(_ != fullName).drop(1).headOption
  }

  private def getArg(args: Seq[String])(name: String): String =
    findArg(args)(name).getOrElse(sys.error(s"missing argument $name"))
}
