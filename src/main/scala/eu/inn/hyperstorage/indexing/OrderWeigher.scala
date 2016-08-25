package eu.inn.hyperstorage.indexing

import eu.inn.hyperbus.model.utils.SortBy
import eu.inn.hyperstorage.api.{HyperStorageIndexSortItem, HyperStorageIndexSortOrder}

object OrderWeigher {
  def weighOrdering(query: Seq[SortBy], indexSortedBy: Seq[HyperStorageIndexSortItem]): Int = {
    val v = indexSortedBy.toVector
    query.foldLeft((0 /*weight*/, 0 /*index*/)) {
      case ((0, 0), sortBy) ⇒ (weighOrdering(sortBy, indexSortedBy.head),1)
      case ((5,index), sortBy) ⇒
        if (v.size > index) {
          val w = weighOrdering(sortBy, v(index))
          (if (w == 10) 3 else w, index+1)
        }
        else {
          (3, index+1)
        }
      case ((10,index), sortBy) ⇒
        if (v.size > index) {
          val w = weighOrdering(sortBy, v(index))
          (if (w == 5) 3 else w, index+1)
        }
        else {
          (3, index+1)
        }
      case ((weight,index), _) ⇒ (weight,index+1)
    }._1
  }

  def weighOrdering(queryEl: SortBy, indexEl: HyperStorageIndexSortItem): Int = {
    if (queryEl.fieldName == indexEl.fieldName) {
      if ((indexEl.order.getOrElse(HyperStorageIndexSortOrder.ASC)==HyperStorageIndexSortOrder.DESC) == queryEl.descending) {
        10
      }
      else {
        5
      }
    }
    else {
      - 10
    }
  }
}
