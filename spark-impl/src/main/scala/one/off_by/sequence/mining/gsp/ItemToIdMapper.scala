package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.Domain.SupportCount
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object ItemToIdMapper {
  def createMappings[ItemType : ClassTag, @specialized(Int, Long, Float, Double) TimeType, SequenceId](
    transactions: RDD[Transaction[ItemType, TimeType, SequenceId]]
  ): RDD[(ItemType, Int)] = {
    transactions
      .flatMap(_.items)
      .distinct
      .zipWithUniqueId()
      .mapValues { id =>
        assert(id >= Int.MinValue && id <= Int.MaxValue)
        id.toInt
      }
  }

  def mapIn[ItemType : ClassTag, @specialized(Int, Long, Float, Double) TimeType, SequenceId](
    transactions: RDD[Transaction[ItemType, TimeType, SequenceId]],
    mappings: RDD[(ItemType, Int)]
  ): RDD[Transaction[Int, TimeType, SequenceId]] = {
    transactions
      .zipWithUniqueId
      .mapValues(_.toInt)
      .flatMap { case (transaction, id) =>
        transaction.items
          .map(item => (item, (id, transaction.time, transaction.sequenceId)))
      }
      .join(mappings)
      .map { case (_, ((id, time, sequenceId), itemId)) =>
        (id, Transaction[Int, TimeType, SequenceId](sequenceId, time, Set(itemId)))
      }
      .reduceByKey { case (a, b) =>
        Transaction[Int, TimeType, SequenceId](a.sequenceId, a.time, a.items ++ b.items)
      }
      .values
  }


  def mapOut[ItemType : ClassTag, @specialized(Int, Long, Float, Double) TimeType, SequenceId](
    patterns: RDD[(Pattern[Int], SupportCount)],
    mappings: RDD[(ItemType, Int)]
  ): RDD[(Pattern[ItemType], SupportCount)] = {
    val reverseMappings = mappings map { case (item, id) => (id, item) }

    patterns
      .zipWithUniqueId
      .flatMap { case ((pattern, support), id) =>
        pattern.elements
          .zipWithIndex
          .flatMap { case (element, index) =>
            element.items
              .map(item => (item, (index, id, support)))
          }
      }
      .join(reverseMappings)
      .map { case (_, ((index, id, support), itemType)) =>
        (id, (support, Vector((index, itemType))))
      }
      .groupByKey
      .values
      .map { itemsInPattern =>
        val support = itemsInPattern.head._1
        val items = itemsInPattern.flatMap(_._2)

        val elements = items
          .groupBy { case (index, _) => index }
          .mapValues(_.map { case (_, item) => item })
          .toVector
          .sortBy { case (index, _) => index }
          .map { case (_, itemsInElement) => itemsInElement.toSet }

        (Pattern(elements.map(Element(_))), support)
      }
  }
}
