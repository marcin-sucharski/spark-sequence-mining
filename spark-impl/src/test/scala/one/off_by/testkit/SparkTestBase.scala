package one.off_by.testkit

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, Suite}

trait SparkTestBase extends BeforeAndAfter {
  this: Suite =>

  protected def sc: SparkContext = sparkContext

  private var sparkContext: SparkContext = _

  before {
    import one.off_by.sequence.mining.gsp.GSP.SparkConfHelper

    val conf: SparkConf = new SparkConf()
      .setAppName("GSP")
      .set("spark.driver.host", "localhost")
      .set("spark.default.parallelism", "2")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .registerGSPKryoClasses()
      .registerMissingSparkClasses()
      .setMaster("local[*]")
    sparkContext = new SparkContext(conf)
  }

  after {
    sparkContext.stop()
  }
}
