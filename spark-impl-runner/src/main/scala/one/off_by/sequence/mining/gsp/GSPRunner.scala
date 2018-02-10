package one.off_by.sequence.mining.gsp

import grizzled.slf4j.Logging
import one.off_by.sequence.mining.gsp.Domain.SupportCount
import one.off_by.sequence.mining.gsp.readers.{InputReader, SequencePerLineInputReader, TransactionCSVInputReader}
import org.apache.spark.{SparkConf, SparkContext}

class GSPRunner(
  inputReader: InputReader
) extends Logging {

  import GSPHelper.SparkConfHelper

  def main(cmdArgs: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("GSP")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerGSPKryoClasses()
      .registerMissingSparkClasses()
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    try {
      val args = ArgumentParser.parseArguments(cmdArgs)
      val input = inputReader.read(sc, args.inputFile)

      val gsp = new GSP[String, Int, Int, Int](sc)
      gsp.execute(input, args.minSupport, args.minItemsInPattern, None, args.toOptions)
        .sortBy(x => (x._1.elements.map(_.items.size).sum, x._2))
        .map(mapResultToString)
        .saveAsTextFile(args.outputFile)
    } catch {
      case e: ArgumentParserException =>
        logger.error(s"Failed to parse arguments: ${e.reason}")
        System.exit(-1)
    }
  }

  private def mapResultToString(result: (Pattern[String], SupportCount)): String =
    s"${result._2} # ${result._1}"
}

object GSPRunnerSequencePerLine extends GSPRunner(new SequencePerLineInputReader())

object GSPRunnerTransactionCSV extends GSPRunner(new TransactionCSVInputReader())
