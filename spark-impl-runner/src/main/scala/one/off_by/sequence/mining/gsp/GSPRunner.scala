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
      val gsp = new GSP[Int, Int, Int, Int](sc)

      val input = inputReader.read(sc, args.inputFile, Some(gsp.partitioner))
      val mappings = ItemToIdMapper.createMappings(input)
      val inputOptimized = ItemToIdMapper.mapIn(input, mappings)

      val result = gsp.execute(inputOptimized, args.minSupport, args.minItemsInPattern, args.toOptions)
      val resultGeneric = ItemToIdMapper.mapOut(result, mappings)

      resultGeneric
        .sortBy(x => (x._1.elements.map(_.items.size).sum, x._2))
        .map(result => s"${result._2} # ${result._1}")
        .saveAsTextFile(args.outputFile)
    } catch {
      case e: ArgumentParserException =>
        logger.error(s"Failed to parse arguments: ${e.reason}")
        System.exit(-1)
    }
  }
}

object GSPRunnerSequencePerLine extends GSPRunner(new SequencePerLineInputReader())

object GSPRunnerTransactionCSV extends GSPRunner(new TransactionCSVInputReader())
