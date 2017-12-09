package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.Domain.SupportCount
import one.off_by.sequence.mining.gsp.PatternJoiner.{JoinItem, JoinItemExistingElement, JoinItemNewElement, PrefixResult, PrefixSuffixResult, SuffixResult}
import one.off_by.sequence.mining.gsp.PatternMatcher.SearchableSequence
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NonFatal

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
ItemType: ClassTag,
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
  ): RDD[(Pattern[ItemType], SupportCount)] = {
    def maybeFilterOut(itemsCount: Long, result: RDD[(Pattern[ItemType], SupportCount)]) =
      if (itemsCount >= minItemsInPattern) result
      else sc.parallelize(List.empty[(Pattern[ItemType], SupportCount)])

    val sequences: RDD[(SequenceId, TransactionType)] = transactions
      .map(t => (t.sequenceId, t))
      .partitionBy(partitioner)
      .cache()

    val sequenceCount = sequences.keys.distinct.count()
    val minSupportCount = (sequenceCount * minSupport).toLong
    logger.info(s"Total sequence count is $sequenceCount and minimum support count is $minSupportCount.")

    val patternMatcher = createPatternMatcher(maybeOptions, sequences, minSupportCount)

    val initialPatterns = prepareInitialPatterns(sequences, minSupportCount)
      .cache()
    logger.info(s"Got ${initialPatterns.count()} initial patterns.")
    val result = Stream.iterate(State(initialPatterns, initialPatterns, 1L)) { case State(acc, prev, prevLength) =>
      val newLength = prevLength + 1
      logger.info(s"Merging patterns of size $prevLength into $newLength.")
      val candidates = patternJoiner.generateCandidates(prev.map(_._1))
      logger.trace(s"Candidates: ${candidates.map(_.toString).toPrettyList}")
      val phaseResult = patternMatcher.filter(candidates)
        .persist(StorageLevels.MEMORY_AND_DISK)
      logger.info(s"Got ${phaseResult.count()} patterns as result.")
      State(maybeFilterOut(newLength, acc.union(phaseResult)), phaseResult, newLength)
    } takeWhile (!_.lastPattern.isEmpty()) lastOption

    result.map(_.result).getOrElse(sc.parallelize(Nil))
  }

  // Creation of PatternMatcher is extracted into separated method thus is can be specialized
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
  implicit class SparkConfHelper(conf: SparkConf) {
    def registerGSPKryoClasses(): SparkConf =
      conf.registerKryoClasses(Array(
        classOf[Transaction[_, _, _]],
        classOf[one.off_by.sequence.mining.gsp.Transaction$mcI$sp],
        classOf[one.off_by.sequence.mining.gsp.Transaction$mcJ$sp],
        classOf[one.off_by.sequence.mining.gsp.Transaction$mcF$sp],
        classOf[one.off_by.sequence.mining.gsp.Transaction$mcD$sp],
        classOf[Array[Transaction[_, _, _]]],
        classOf[Array[one.off_by.sequence.mining.gsp.Transaction$mcI$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.Transaction$mcJ$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.Transaction$mcF$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.Transaction$mcD$sp]],
        classOf[Taxonomy[_]],
        classOf[Array[Taxonomy[_]]],
        classOf[Element[_]],
        classOf[Array[Element[_]]],
        classOf[Pattern[_]],
        classOf[Array[Pattern[_]]],
        classOf[JoinItem[_]],
        classOf[Array[JoinItem[_]]],
        classOf[JoinItemNewElement[_]],
        classOf[Array[JoinItemNewElement[_]]],
        classOf[JoinItemExistingElement[_]],
        classOf[Array[JoinItemExistingElement[_]]],
        classOf[PrefixSuffixResult[_]],
        classOf[Array[PrefixSuffixResult[_]]],
        classOf[PrefixResult[_]],
        classOf[Array[PrefixResult[_]]],
        classOf[SuffixResult[_]],
        classOf[Array[SuffixResult[_]]],
        classOf[SearchableSequence[_, _, _]],
        classOf[Array[SearchableSequence[_, _, _]]],
        classOf[HashTree[_, _, _, _]],
        classOf[Array[HashTree[_, _, _, _]]],
        classOf[HashTreeLeaf[_, _, _, _]],
        classOf[Array[HashTreeLeaf[_, _, _, _]]],
        classOf[HashTreeNode[_, _, _, _]],
        classOf[Array[HashTreeNode[_, _, _, _]]]
      ))

    def registerMissingSparkClasses(): SparkConf =
      conf.registerKryoClasses(Array(
        Class.forName("scala.reflect.ClassTag$$anon$1"),
        classOf[Class[_]],
        classOf[mutable.WrappedArray.ofRef[_]],
        Ordering.Int.getClass,
        Ordering.String.getClass
      ))
  }
}

object GSPMain {
  import GSP.SparkConfHelper

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("GSP")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .registerGSPKryoClasses()
      .registerMissingSparkClasses()
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val inputFile = getArg(args)("in")
    val outputFile = getArg(args)("out")
    val minSupport = findArg(args)("min-support").map(_.toDouble).getOrElse(0.5)
    val minItemsInPattern = findArg(args)("min-items-in-pattern").map(_.toInt).getOrElse(1)
    val windowSize = findArg(args)("window-size").map(_.toInt)
    val minGap = findArg(args)("min-gap").map(_.toInt)
    val maxGap = findArg(args)("max-gap").map(_.toInt)

    val gsp = new GSP[Int, Int, Int, Int](sc)
    val maybeOptions = Some(GSPOptions(longTypeSupport, windowSize, minGap, maxGap))
      .filter(_ => windowSize.isDefined || minGap.isDefined || maxGap.isDefined)

    val input = sc.textFile(inputFile)
      .repartition(gsp.partitioner.numPartitions)
      .filter(_.trim.nonEmpty)
      .flatMap(parseLineIntoTransaction)

    gsp.execute(input, minSupport, minItemsInPattern, None, maybeOptions)
      .sortBy(x => (x._1.elements.map(_.items.size).sum, x._2))
      .map(mapResultToString)
      .saveAsTextFile(outputFile)

    Console.println("Press any key to finish")
    Console.in.readLine()
  }

  def parseLineIntoTransaction(line: String): Seq[Transaction[Int, Int, Int]] =
    (try line.split("-1").map(_.split(" ").map(_.trim).filter(_.nonEmpty).map(_.toInt).toList).toList catch {
      case NonFatal(e) =>
        sys.error(s"malformed line due to ${e.getLocalizedMessage}: $line")
    }) match {
      case (sequenceId :: Nil) :: itemsets =>
        itemsets.filter(_.nonEmpty).zipWithIndex map { case (items, index) =>
          Transaction[Int, Int, Int](sequenceId, index + 1, items.toSet)
        }

      case _ =>
        sys.error(s"malformed line: $line")
    }

  def mapResultToString(result: (Pattern[Int], SupportCount)): String =
    s"${result._2} # ${result._1}"

  val longTypeSupport: GSPTypeSupport[Int, Int] = GSPTypeSupport[Int, Int](
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
