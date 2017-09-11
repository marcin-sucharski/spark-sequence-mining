package one.off_by.testkit

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, Suite}

trait SparkTestBase extends BeforeAndAfter {
  this: Suite =>

  protected def sc: SparkContext = sparkContext

  private var sparkContext: SparkContext = _

  before {
    val conf: SparkConf = new SparkConf()
      .setAppName("GSP")
      .set("spark.driver.host", "localhost")
      .set("spark.default.parallelism", "2")
      .setMaster("local[*]")
    sparkContext = new SparkContext(conf)
  }

  after {
    sparkContext.stop()
  }
}
