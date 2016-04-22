package com.gravity.utilities

import java.util
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.reflect.ClassTag

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/17/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

//based on a combination of the overlock project (https://github.com/boundary/overlock), which had Far More than We Needed, and wasn't maintained, and https://gist.github.com/brikis98/5843195
class GrvConcurrentMap[A, B: ClassTag](initialCapacity: Int = 16, loadFactor: Float = 0.75f, concurrencyLevel: Int = 16)
  extends scala.collection.concurrent.Map[A, B] {

  private class GrvConcurrentMapValueWrapper[T: ClassTag](op: => T) {
    lazy val value: T = op

    override def equals(other: Any): Boolean = other match {
      case o: GrvConcurrentMapValueWrapper[_] => value == o.value
      case v: T => value == v
      case _ => false
    }
  }

  private val internalMap = new ConcurrentHashMap[A, GrvConcurrentMapValueWrapper[B]](initialCapacity, loadFactor, concurrencyLevel)

  override def -=(key: A): this.type = {
    internalMap.remove(key)
    this
  }

  override def +=(kv: (A, B)): this.type = {
    internalMap.put(kv._1, new GrvConcurrentMapValueWrapper(kv._2))
    this
  }

  override def iterator: Iterator[(A, B)] = {
    new Iterator[(A, B)] {
      val iter: util.Iterator[Entry[A, GrvConcurrentMapValueWrapper[B]]] = internalMap.entrySet().iterator()

      def next(): (A, B) = {
        val entry = iter.next()
        (entry.getKey, entry.getValue.value)
      }

      def hasNext: Boolean = iter.hasNext
    }
  }

  override def get(key: A): Option[B] = Option(internalMap.get(key)).map(_.value)

  override def replace(k: A, v: B): Option[B] = Option(internalMap.replace(k, new GrvConcurrentMapValueWrapper(v))).map(_.value)

  override def replace(k: A, oldvalue: B, newvalue: B): Boolean = internalMap.replace(k, new GrvConcurrentMapValueWrapper(oldvalue), new GrvConcurrentMapValueWrapper(newvalue))

  override def remove(k: A, v: B): Boolean = internalMap.remove(k, new GrvConcurrentMapValueWrapper(v))

  override def putIfAbsent(k: A, v: B): Option[B] = Option(internalMap.putIfAbsent(k, new GrvConcurrentMapValueWrapper(v))).map(_.value)

  // This is like putIfAbsent, but always returns the value that ended up (or was already) associated with the key.
  override def getOrElseUpdate(k: A, op: => B): B = {
    val t = new GrvConcurrentMapValueWrapper(op)

    Option(internalMap.putIfAbsent(k, t)).getOrElse(t).value
  }
}

class GrvConcurrentSet[T] extends mutable.Set[T] {
  private val map = new GrvConcurrentMap[T, Unit]()

  override def +=(elem: T): GrvConcurrentSet.this.type = {
    map.update(elem, Unit)
    this
  }

  override def -=(elem: T): GrvConcurrentSet.this.type = {
    map.remove(elem)
    this
  }

  override def contains(elem: T): Boolean = map.contains(elem)

  override def foreach[U](f: T =>  U) = map.keys.foreach[U](f)

  override def size : Int = map.size

  override def +(elem: T): GrvConcurrentSet.this.type = clone().asInstanceOf[GrvConcurrentSet.this.type] += elem

  override def -(elem: T): GrvConcurrentSet.this.type = clone().asInstanceOf[GrvConcurrentSet.this.type] -= elem

  override def iterator: Iterator[T] = map.keysIterator

  override def clear() = map.clear()
}