import scala.io.{Source, StdIn}

final case class Row(
  userId: Int,
  orderId: Int,
  orderNumber: Int,
  daysSincePriorOrder: Option[Int],
  productName: String,
  orderHourOfDay: Int
)

final case class Transaction(
  userId: Int,
  orderNumber: Int,
  daysSincePriorOrder: Option[Int],
  orderHourOfDay: Int,
  items: Set[String]
)

final case class TimedTransaction(
  userId: Int,
  baseTime: Int,
  time: Int,
  items: Set[String]
) {
  override def toString: String = s"$userId,$time,${items.mkString(",")}"
}

def splitCSVLine(input: String): List[String] = {
  type Rest = String
  type Item = String

  def parseQuoted(rest: String): (Rest, Item) = {
    assert(rest.startsWith("\""))
    val (item, unparsed) = rest.split(",").span(_.last != '"')

    val content = (item.mkString(",") + unparsed.head).filter(_ != '"')
    (unparsed.tail.mkString(","), '"' + s"$content" + '"')
  }

  def parseSimple(rest: String): (Rest, Item) = {
    val (item, unparsed) = rest.span(_ != ',')
    if (unparsed.nonEmpty) (unparsed.tail, item)
    else ("", item)
  }

  def parseItem(rest: String): (Rest, Item) = {
    val realItem = rest.dropWhile(_.isWhitespace)
    if (realItem.startsWith("\"")) parseQuoted(realItem)
    else parseSimple(realItem)
  }

  def parseLine(line: String): List[Item] =
    if (line.nonEmpty) {
      val (rest, item) = parseItem(line)
      item :: parseLine(rest)
    } else Nil

  parseLine(input)
}

def parseLine(line: String): Row = splitCSVLine(line) match {
  case userId :: orderId :: orderNumber :: daysSincePriorOrder :: productName :: orderHourOfDay :: Nil =>
    Row(
      userId.toInt,
      orderId.toInt,
      orderNumber.toInt,
      Some(daysSincePriorOrder).filter(_.nonEmpty).map(_.toInt),
      productName,
      hourToInt(orderHourOfDay)
    )

  case splitted =>
    sys.error(s"malformed line: $line. Splitted: ${splitted.mkString("\n")}")
}

def hourToInt(orderHourOfDay: String): Int =
  Some(orderHourOfDay.dropWhile(_ == '0')).filter(_.nonEmpty).map(_.toInt).getOrElse(0)

def rowsToTransaction(rows: Iterable[Row]): Transaction =
  Transaction(
    userId = rows.head.userId,
    orderNumber = rows.head.orderNumber,
    daysSincePriorOrder = rows.head.daysSincePriorOrder,
    orderHourOfDay = rows.head.orderHourOfDay,
    items = rows.map(_.productName).toSet
  )

Stream
  .continually(StdIn.readLine)
  .takeWhile(_ != null)
  .toParArray
  .map(parseLine)
  .groupBy(_.userId)
  .values
  .flatMap {
    _
      .groupBy(_.orderId)
      .mapValues(_.toList)
      .mapValues(rowsToTransaction)
      .values.toList
      .sortBy(_.orderNumber)
      .foldLeft(List.empty[TimedTransaction]) { case (agg, transaction) =>
        val previous = agg.headOption
        val offset = transaction.daysSincePriorOrder.map(_ * 24).getOrElse(0)
        TimedTransaction(
          userId = transaction.userId,
          baseTime = previous.map(_.baseTime).getOrElse(0) + offset,
          time = previous.map(_.time).getOrElse(0) + offset + transaction.orderHourOfDay,
          items = transaction.items
        ) :: agg
      }
  }
  .foreach(println)
