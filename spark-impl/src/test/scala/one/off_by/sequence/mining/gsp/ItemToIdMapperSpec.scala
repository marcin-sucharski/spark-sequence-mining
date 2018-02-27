package one.off_by.sequence.mining.gsp

import one.off_by.testkit.SparkTestBase
import org.scalatest.{Inspectors, Matchers, WordSpec}

class ItemToIdMapperSpec extends WordSpec
  with Matchers
  with Inspectors
  with SparkTestBase {

  "ItemToIdMapper" should {
    "create correct set of mappings" in {
      val mappings = ItemToIdMapper.createMappings(sc.parallelize(transactions)).collect()

      mappings.map(_._1) should contain theSameElementsAs items
      mappings should have (length (items.length))
      mappings.map(_._2).distinct should have (length (items.length))
    }

    "correctly maps items from transactions to ids" in {
      val transactionsRDD = sc.parallelize(transactions)
      val mappingsRDD = sc.parallelize(mappings)

      ItemToIdMapper.mapIn(transactionsRDD, mappingsRDD).collect() should contain theSameElementsAs List(
        Transaction(1, 1, Set(1, 2, 3)),
        Transaction(1, 2, Set(1, 2, 3)),
        Transaction(1, 3, Set(1, 2, 3)),
        Transaction(1, 4, Set(1, 2, 3)),
        Transaction(1, 5, Set(1, 2, 3)),
        Transaction(1, 6, Set(1, 2, 3)),
        Transaction(1, 7, Set(1, 2, 3)),

        Transaction(2, 1, Set(1, 2, 3)),
        Transaction(2, 2, Set(1, 2, 3)),
        Transaction(2, 3, Set(1, 2, 3)),
        Transaction(2, 4, Set(1, 2, 3)),
        Transaction(2, 5, Set(1, 2, 3)),
        Transaction(2, 6, Set(1, 2, 3)),
        Transaction(2, 7, Set(1, 2, 3)),

        Transaction(3, 1, Set(4)),
        Transaction(3, 2, Set(5)),
        Transaction(3, 3, Set(6))
      )
    }

    "correctly maps ids from patterns to items" in {
      val supportCounts = List(1, 2)
      val patterns = List[Pattern[Int]](
        Pattern(Vector(Element(1, 2, 3), Element(1), Element(2))),
        Pattern(Vector(Element(4), Element(5), Element(6)))
      )
      val expectedPatterns = List[Pattern[String]](
        Pattern(Vector(Element("a", "b", "c"), Element("a"), Element("b"))),
        Pattern(Vector(Element("1"), Element("2"), Element("3")))
      ) zip supportCounts

      val patternsRDD = sc.parallelize(patterns zip supportCounts)
      val mappingsRDD = sc.parallelize(mappings)

      ItemToIdMapper.mapOut(patternsRDD, mappingsRDD).collect() should contain theSameElementsAs expectedPatterns
    }
  }

  private val items = List("a", "b", "c", "1", "2", "3")

  private val transactions = List[Transaction[String, Int, Int]](
    Transaction(1, 1, Set("a", "b", "c")),
    Transaction(1, 2, Set("a", "b", "c")),
    Transaction(1, 3, Set("a", "b", "c")),
    Transaction(1, 4, Set("a", "b", "c")),
    Transaction(1, 5, Set("a", "b", "c")),
    Transaction(1, 6, Set("a", "b", "c")),
    Transaction(1, 7, Set("a", "b", "c")),

    Transaction(2, 1, Set("a", "b", "c")),
    Transaction(2, 2, Set("a", "b", "c")),
    Transaction(2, 3, Set("a", "b", "c")),
    Transaction(2, 4, Set("a", "b", "c")),
    Transaction(2, 5, Set("a", "b", "c")),
    Transaction(2, 6, Set("a", "b", "c")),
    Transaction(2, 7, Set("a", "b", "c")),

    Transaction(3, 1, Set("1")),
    Transaction(3, 2, Set("2")),
    Transaction(3, 3, Set("3"))
  )

  private val mappings = List(
    "a" -> 1,
    "b" -> 2,
    "c" -> 3,
    "1" -> 4,
    "2" -> 5,
    "3" -> 6
  )
}
