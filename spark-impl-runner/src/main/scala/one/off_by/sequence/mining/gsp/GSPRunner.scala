package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.Domain.SupportCount
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.NonFatal

object GSPRunner {
  import GSPHelper.SparkConfHelper

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

    println("Finished")
  }

  private def parseLineIntoTransaction(line: String): Seq[Transaction[Int, Int, Int]] =
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

  private def mapResultToString(result: (Pattern[Int], SupportCount)): String =
    s"${result._2} # ${result._1}"

  private val longTypeSupport: GSPTypeSupport[Int, Int] = GSPTypeSupport[Int, Int](
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
