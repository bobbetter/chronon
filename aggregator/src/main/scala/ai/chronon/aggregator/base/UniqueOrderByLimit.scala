package ai.chronon.aggregator.base

import java.util
import java.util.Comparator

object UniqueOrderByLimit {

  def initState[T, OrderType]: State[T, OrderType] =
    State(new util.ArrayList[T](), new util.HashSet[Long](), null.asInstanceOf[OrderType])

  case class State[T, OrderType](elems: java.util.ArrayList[T],
                                 ids: java.util.HashSet[Long],
                                 var orderWaterMark: OrderType)

  // UniqueTopKAggregator will create this Operator from input args and call the public methods in its implementation
  case class Operator[T, OrderType: Ordering](getOrderKey: T => OrderType,
                                              getId: T => Long,
                                              k: Int,
                                              maxSize: Int,
                                              topK: Boolean = true) {

    private val ordering = implicitly[Ordering[OrderType]]

    // to be used in "finalize" of the aggregator
    def sortAndPrune(state: State[T, OrderType]): Unit = {

      sort(state)
      val elems = state.elems

      while (elems.size > k) {
        val removed = elems.remove(elems.size - 1)
        state.ids.remove(getId(removed))
      }

      if (elems.size > 0) {
        state.orderWaterMark = getOrderKey(elems.get(elems.size - 1))
      }
    }

    // to be used to impl "denormalize" of the aggregator
    def buildStateFromElems(elems: java.util.ArrayList[T]): State[T, OrderType] = {

      val state: State[T, OrderType] = UniqueOrderByLimit.initState[T, OrderType]
      val it = elems.iterator()

      while (it.hasNext) {
        insert(it.next(), state)
      }

      state
    }

    def insert(elem: T, state: State[T, OrderType]): Unit = {

      // dedup first
      if (state.ids.contains(getId(elem))) return

      val orderKey = getOrderKey(elem)

      if (topK) {
        if (state.elems.size() < k) {

          state.orderWaterMark = if (state.orderWaterMark == null || ordering.lt(orderKey, state.orderWaterMark)) {
            orderKey
          } else {
            state.orderWaterMark
          }

          state.elems.add(elem)
          state.ids.add(getId(elem))

        } else if (ordering.gt(orderKey, state.orderWaterMark)) {

          state.elems.add(elem)
          state.ids.add(getId(elem))

        }
      } else {
        if (state.elems.size() < k) {
          // keep min
          state.orderWaterMark = if (state.orderWaterMark == null || ordering.gt(orderKey, state.orderWaterMark)) {
            orderKey
          } else {
            state.orderWaterMark
          }

          state.elems.add(elem)
          state.ids.add(getId(elem))

        } else if (ordering.lt(orderKey, state.orderWaterMark)) {

          state.elems.add(elem)
          state.ids.add(getId(elem))

        }
      }

      if (state.elems.size() > maxSize) {
        sortAndPrune(state)
      }
    }

    private def sort(state: State[T, OrderType]): Unit = {

      state.elems.sort(new Comparator[T] {
        override def compare(o1: T, o2: T): Int = {
          val o1Key = getOrderKey(o1)
          val o2Key = getOrderKey(o2)

          // sort desc when topK or sort asc when bottomK
          if (topK) {
            // descending
            ordering.compare(o2Key, o1Key)
          } else {
            // ascending
            ordering.compare(o1Key, o2Key)
          }
        }
      })
    }
  }

}
