/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.timo.temporal

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class Node[T: ClassTag](value:Array[Long], time:Array[Long]) extends Serializable {

  var leftChild: Node[T] = null
  var rightChild: Node[T] = null
  val minValue=value(0)
  val maxValue=value(1)

  val minTime:Long=time(0)
  val maxTime:Long=time(1)

  var NodeMax=time(1)
  var NodeMin=time(0)

  // DATA SHOULD BE STORED MORE EFFICIENTLY
  var data: ListBuffer[T] = new ListBuffer()

  def this(data: T,zz:Array[Long],zx:Array[Long]) = {
    this(zz,zx)
    put(data)
  }

  def getSize(): Long = {
    data.length
  }

  def print(): Unit ={
    data.foreach(println)
  }

  def clearChildren() = {
    leftChild = null
    rightChild = null
  }

  def multiput(rs: Iterator[T]) = {
    val newData = rs.toList
    data ++= newData
  }

  def multiput(rs: List[T]) = {
    data ++= rs
  }

  def put(newData: T) = {
    data += newData
  }

  def get(): Iterator[T] = data.toIterator

  def BiggerThan(other: Long): Boolean = {
      minValue>other
  }

  def SmallerThan(other: Long): Boolean = {
      maxValue<other
  }

  override def clone: Node[T] = {
    val n: Node[T] = new Node(Array(minValue,maxValue),Array(minTime,maxTime))
    n.data = data
    n
  }

  def overlaps(other: Long): Boolean = {
    (minValue<=other && maxValue>=other)
  }

}
