package com.gravity.hbase.schema

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Mutation, Row}

import scala.collection.JavaConversions._
import scala.collection.mutable

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * An individual data modification operation (put, increment, or delete usually)
 * These operations are chained together by the client, and then executed in bulk.
 * @param table
 * @param key
 * @param previous
 * @tparam T
 * @tparam R
 */
abstract class OpBase[T <: HbaseTable[T, R, _], R](val table: HbaseTable[T, R, _], key: Array[Byte], val previous: mutable.Buffer[OpBase[T, R]] = mutable.Buffer[OpBase[T, R]]()) {

  previous += this

  def +(that:OpBase[T,R]) : OpBase[T,R]

  def put(key: R, writeToWAL: Boolean = true): PutOp[T, R] = {
    val po = new PutOp(table, table.rowKeyConverter.toBytes(key), previous, writeToWAL)
    po
  }

  def increment(key: R): IncrementOp[T, R] = {
    val inc = new IncrementOp(table, table.rowKeyConverter.toBytes(key), previous)
    inc
  }

  def delete(key: R): DeleteOp[T, R] = {
    val del = new DeleteOp(table, table.rowKeyConverter.toBytes(key), previous)
    del
  }

  def size: Int = previous.size

  def getOperations: Iterable[Mutation] = {
    val calls = mutable.Buffer[Mutation]()
    previous.foreach {
      case put: PutOp[T, R] =>
        calls += put.put
      case delete: DeleteOp[T, R] =>
        calls += delete.delete
      case increment: IncrementOp[T, R] =>
        calls += increment.increment
    }

    calls

  }

  def prepareOperations: (mutable.Buffer[Row], Int, Int, Int) = {
    val ops = mutable.Buffer[Row]()

    var puts = 0
    var increments = 0
    var deletes = 0

    previous.foreach {
      case put: PutOp[T, R] =>
        if (!put.put.isEmpty) {
          ops += put.put
          puts += 1
        }
      case delete: DeleteOp[T, R] =>
        ops += delete.delete
        deletes += 1
      case increment: IncrementOp[T, R] =>
        ops += increment.increment
        increments += 1
    }

    (ops, puts, deletes, increments)
  }

  def execute(tableName: String = table.tableName, timeOutMs : Int = 0)(implicit conf: Configuration): OpsResult = {
    val (ops, puts, deletes, increments) = prepareOperations

    if (ops.isEmpty) {
      //No need to do anything if there are no real operations to execute
    } else {
      table.withTable(tableName, conf, timeOutMs) {
        table =>
          table.batch(ops)

//          if (puts.size > 0) {
//            table.put(puts)
//            //IN THEORY, the operations will happen in order.  If not, break this into two different batched calls for deletes and puts
//          }
//          if (deletes.size > 0) {
//            table.delete(deletes)
//          }
//          if (increments.size > 0) {
//            increments.foreach(increment => table.increment(increment))
//          }
      }
    }


    OpsResult(deletes, puts, increments)
  }
}

case class OpsResult(numDeletes: Int, numPuts: Int, numIncrements: Int) {
  def +(that: OpsResult): OpsResult = OpsResult(numDeletes + that.numDeletes, numPuts + that.numPuts, numIncrements + that.numIncrements)
}
