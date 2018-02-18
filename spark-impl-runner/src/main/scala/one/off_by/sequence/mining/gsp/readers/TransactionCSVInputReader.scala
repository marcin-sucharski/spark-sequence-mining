package one.off_by.sequence.mining.gsp.readers

import com.github.tototoshi.csv.{CSVFormat, CSVParser, DefaultCSVFormat, Quoting}
import one.off_by.sequence.mining.gsp.Transaction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class TransactionCSVInputReader extends InputReader {
  override def read(sc: SparkContext, inputPath: String): RDD[Transaction[String, Int, Int]] =
    sc.textFile(inputPath)
      .filter(_.trim.nonEmpty)
      .map(TransactionCSVInputReader.parseLineIntoTransaction)
}

object TransactionCSVInputReader {
  private val format: CSVFormat = new DefaultCSVFormat {
    override val escapeChar: Char = '\\'
    override val lineTerminator: String = "\n"
  }

  private val parser: CSVParser = new CSVParser(format)

  def parseLineIntoTransaction(line: String): Transaction[String, Int, Int] =
    parser.parseLine(line) match {
      case Some(sequenceId :: time :: items) => Transaction(sequenceId.toInt, time.toInt, items.toSet)
      case _ => sys.error(s"malformed line: $line")
    }
}
