package one.off_by.sequence.mining.gsp.utils

import scala.reflect.ClassTag
import scala.util.control.Breaks._

object CollectionDistinctUtils {
  implicit class IteratorDistinct[T >: Null](iterator: Iterator[T])(implicit ct: ClassTag[T]) {
    def distinct: Iterator[T] = {
      var size = baseArraySize
      val set = new Array[Array[T]](maxSetLevels)
      set(0) = new Array[T](size)
      var top = 0

      while (iterator.hasNext) {
        val item = iterator.next()
        val hashCode = item.hashCode()

        var current = top
        breakable {
          while (current >= 0) {
            val array = set(current)
            val index = hashCode & (array.length - 1)
            if (array(index) == null || array(index) == item) {
              array(index) = item
              break
            } else {
              current -= 1
            }
          }
        }

        if (current < 0) {
          size *= sizeMultiplier
          val nextArray = new Array[T](size)
          nextArray(hashCode & (size - 1)) = item
          top += 1
          set(top) = nextArray
        }
      }


      new Iterator[T] {
        private[this] var nextIndex = 0

        override def hasNext: Boolean = {
          while (top >= 0 && set(top)(nextIndex) == null) {
            if (!findNextInArray()) {
              set(top) = null
              top -= 1
              nextIndex = 0
            }
          }

          top >= 0 && set(top)(nextIndex) != null
        }

        override def next(): T =
          if (hasNext) {
            val item = set(top)(nextIndex)
            var current = top
            while (current >= 0) {
              val index = nextIndex & (set(current).length - 1)
              if (set(current)(index) == item) {
                set(current)(index) = null
              }
              current -= 1
            }
            item
          } else Iterator.empty.next()

        private def findNextInArray(): Boolean = {
          val array = set(top)
          while (nextIndex < array.length && array(nextIndex) == null) {
            nextIndex += 1
          }
          nextIndex < array.length
        }
      }
    }
  }

  private[this] val maxSetLevels = 16
  private[this] val sizeMultiplier = 2
  private[this] val baseArraySize = 32
}
