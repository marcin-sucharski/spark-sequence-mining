package one.off_by.sequence.mining.gsp.utils

import scala.reflect.ClassTag

object CollectionDistinctUtils {
  implicit class IteratorDistinct[T >: Null](iterator: Iterator[T])(implicit ct: ClassTag[T]) {
    def distinct: Iterator[T] = {
      val state = prepareInitialArraysSet()

      insertItemsIntoArraysSet(state)
      makeIterator(state)
    }

    @inline
    private[this] final def prepareInitialArraysSet(): ArraySetState[T] = {
      val set = new Array[Array[T]](maxSetLevels)
      set(0) = new Array[T](baseArraySize)
      ArraySetState(set, baseArraySize, 0)
    }

    @inline
    private[this] final def insertItemsIntoArraysSet(state: ArraySetState[T]): Unit = {
      type SuccessfullyInserted = Boolean

      @inline
      def tryInsertItem(item: T, hashCode: Int): SuccessfullyInserted = {
        var current = state.topIndex

        while (current >= 0) {
          val array = state.arrays(current)
          val index = hashCode & (array.length - 1)
          if (array(index) == null || array(index) == item) {
            array(index) = item
            return true
          } else {
            current -= 1
          }
        }

        false
      }

      @inline
      def extendArraySet(item: T, hashCode: Int): Unit = {
        state.lastLayerSize *= sizeMultiplier
        val nextArray = new Array[T](state.lastLayerSize)
        nextArray(hashCode & (state.lastLayerSize - 1)) = item
        state.topIndex += 1
        state.arrays(state.topIndex) = nextArray
      }

      while (iterator.hasNext) {
        val item = iterator.next()
        val hashCode = item.hashCode()

        if (!tryInsertItem(item, hashCode)) {
          extendArraySet(item, hashCode)
        }
      }
    }

    @inline
    private[this] final def makeIterator(state: ArraySetState[T]): Iterator[T] =
      new Iterator[T] {
        private[this] var nextIndex = 0

        override def hasNext: Boolean = {
          while (state.topIndex >= 0 && state.arrays(state.topIndex)(nextIndex) == null) {
            if (!findNextInArray()) {
              state.arrays(state.topIndex) = null
              state.topIndex -= 1
              nextIndex = 0
            }
          }

          state.topIndex >= 0 && state.arrays(state.topIndex)(nextIndex) != null
        }

        override def next(): T =
          if (hasNext) {
            val item = state.arrays(state.topIndex)(nextIndex)
            var current = state.topIndex
            while (current >= 0) {
              val index = nextIndex & (state.arrays(current).length - 1)
              if (state.arrays(current)(index) == item) {
                state.arrays(current)(index) = null
              }
              current -= 1
            }
            item
          } else Iterator.empty.next()

        private def findNextInArray(): Boolean = {
          val array = state.arrays(state.topIndex)
          while (nextIndex < array.length && array(nextIndex) == null) {
            nextIndex += 1
          }
          nextIndex < array.length
        }
      }
  }

  private[this] final case class ArraySetState[T](
    arrays: Array[Array[T]],
    var lastLayerSize: Int,
    var topIndex: Int
  )

  private[this] val maxSetLevels = 32
  private[this] val sizeMultiplier = 2
  private[this] val baseArraySize = 32
}
