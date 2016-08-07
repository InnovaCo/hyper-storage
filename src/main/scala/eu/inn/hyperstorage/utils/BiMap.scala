package eu.inn.hyperstorage.utils

import scala.collection.mutable

class BiMap[A,B](forward:mutable.Map[A,B], backward:mutable.Map[B,A]) extends mutable.Map[A,B] {
  def inverse = new BiMap(backward, forward)

  def get(key:A) = forward get key

  def iterator = forward.iterator

  def +=(kv:(A,B)) = {
    forward += kv
    backward += kv.swap
    this
  }

  def -=(key:A) = {
    backward --= (forward get key)
    forward -= key
    this
  }

  override def empty = {
    forward.empty
    backward.empty
    this
  }

  override def size = forward.size
}

object BiMap {
  def apply[A,B](elems:(A,B)*) = new BiMap[A,B](mutable.Map.empty, mutable.Map.empty) ++= elems
}
