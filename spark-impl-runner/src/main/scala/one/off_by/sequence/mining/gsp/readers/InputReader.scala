package one.off_by.sequence.mining.gsp.readers

import one.off_by.sequence.mining.gsp.Transaction
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

trait InputReader {
  def read(
    sc: SparkContext,
    inputPath: String,
    maybePartitioner: Option[Partitioner] = None
  ): RDD[Transaction[String, Int, Int]]
}
