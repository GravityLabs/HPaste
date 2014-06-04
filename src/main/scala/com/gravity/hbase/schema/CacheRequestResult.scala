package com.gravity.hbase.schema

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 5/14/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

sealed abstract class CacheRequestResult[+A] extends Product with Serializable {
  self =>

  def hasValue : Boolean //returns true if there was something stored at all
  def isEmptyValue : Boolean //returns true if there was something stored, but it is empty
  def get : A
  def toOption : Option[A]
}

final case class Found[+A](x: A) extends CacheRequestResult[A] {
  def hasValue = true
  def isEmptyValue = false
  def get = x
  def toOption = Some(x)
}

case object FoundEmpty extends CacheRequestResult[Nothing] {
  def hasValue = true
  def isEmptyValue = true
  def get = throw new NoSuchElementException("FoundEmpty.get")
  def toOption = None
}

case object NotFound extends CacheRequestResult[Nothing] {
  def hasValue = false
  def isEmptyValue = false
  def get = throw new NoSuchElementException("NotFound.get")
  def toOption = None
}

final case class Error(message: String, exceptionOption: Option[Throwable]) extends CacheRequestResult[Nothing] {
  def hasValue = false
  def isEmptyValue = false
  def get = throw new NoSuchElementException("Error.get")
  def toOption = None
}
