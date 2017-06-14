package one.off_by.sequence.mining.generator

object DataGenerator {
  def main(args: Array[String]): Unit = {

  }

  case class Transaction(
    time: Int,
    customerId: Int,
    items: List[String]
  )
}
