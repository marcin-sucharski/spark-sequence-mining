package one.off_by.sequence.mining.gsp

import java.io.{File, PrintWriter}

import grizzled.slf4j.Logging
import one.off_by.sequence.mining.gsp.readers.{InputReader, SequencePerLineInputReader, TransactionCSVInputReader}
import org.apache.spark.storage.StorageLevel
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

    val startTimeMillis = System.currentTimeMillis()
    try {
      val args = ArgumentParser.parseArguments(cmdArgs)
      val gsp = new GSP[Int, Int, Int, Int](sc)
      val maybeStatistics = args.statisticsOutput.map(_ => SimpleStatisticsGatherer())

      val input = inputReader.read(sc, args.inputFile, Some(gsp.partitioner))
      val mappings = ItemToIdMapper.createMappings(input).persist(StorageLevel.DISK_ONLY_2)
      val inputOptimized = ItemToIdMapper.mapIn(input, mappings)

      val result = gsp.execute(
        transactions = inputOptimized,
        minSupport = args.minSupport,
        minItemsInPattern = args.minItemsInPattern,
        maybeOptions = args.toOptions,
        statisticsGatherer = maybeStatistics
      )
      val resultGeneric = ItemToIdMapper.mapOut(result, mappings)

      resultGeneric
        .sortBy(x => (x._1.elements.map(_.items.size).sum, x._2))
        .map(result => s"${result._2} # ${result._1}")
        .saveAsTextFile(args.outputFile)

      val totalTimeMillis = System.currentTimeMillis() - startTimeMillis
      for {
        pathToStatisticsOutput <- args.statisticsOutput
        statistics <- maybeStatistics
        writer <- Some(new PrintWriter(new File(pathToStatisticsOutput)))
      } {
        try writer.write(StatisticsSerializer.serializerToJson(statistics, args, totalTimeMillis))
        finally writer.close()
      }
    } catch {
      case e: ArgumentParserException =>
        logger.error(s"Failed to parse arguments: ${e.reason}")
        System.exit(-1)
    }
  }
}

object GSPRunnerSequencePerLine extends GSPRunner(new SequencePerLineInputReader())

object GSPRunnerTransactionCSV extends GSPRunner(new TransactionCSVInputReader())
