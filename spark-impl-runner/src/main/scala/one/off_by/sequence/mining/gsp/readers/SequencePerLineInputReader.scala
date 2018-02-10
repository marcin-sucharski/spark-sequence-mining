package one.off_by.sequence.mining.gsp.readers

import one.off_by.sequence.mining.gsp.Transaction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.control.NonFatal

class SequencePerLineInputReader extends InputReader {
  override def read(sc: SparkContext, inputPath: String): RDD[Transaction[String, Int, Int]] =
    sc.textFile(inputPath)
      .filter(_.trim.nonEmpty)
      .flatMap(SequencePerLineInputReader.parseLineIntoTransactions)
}

object SequencePerLineInputReader {
  private def parseLineIntoTransactions(line: String): Seq[Transaction[String, Int, Int]] =
    (try line.split("-1").map(_.split(" ").map(_.trim).filter(_.nonEmpty).toList).toList catch {
      case NonFatal(e) =>
        sys.error(s"malformed line due to ${e.getLocalizedMessage}: $line")
    }) match {
      case (sequenceId :: Nil) :: itemsets =>
        itemsets.filter(_.nonEmpty).zipWithIndex map { case (items, index) =>
          Transaction[String, Int, Int](sequenceId.toInt, index + 1, items.toSet)
        }

      case _ =>
        sys.error(s"malformed line: $line")
    }
}
