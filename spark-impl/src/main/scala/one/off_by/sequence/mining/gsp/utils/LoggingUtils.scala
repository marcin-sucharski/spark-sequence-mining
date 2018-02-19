package one.off_by.sequence.mining.gsp.utils

import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD

trait LoggingUtils extends Logging {
  implicit class ArrayPrinter[T](array: Array[T]) {
    def toPrettyList: String = array.mkString("\n", "\n", "\n")
  }

  implicit class RDDPrinter[T](rdd: RDD[T]) {
    def toPrettyList: String = rdd.collect().toPrettyList
  }
}
