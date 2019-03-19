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

// k = type for entity id, T = value type stored in hashmap
class IntervalTree[T: ClassTag] extends Serializable {
  var root: Node[T] = _
  var leftDepth: Long = 0
  var rightDepth: Long = 0
  val threshold = 1
  var nodeCount: Long = 0
  var sorted:ListBuffer[T]=new ListBuffer[T]()

  def get(): List[T] = {
    inOrder().flatMap(r => r.get().toList)
  }

  def countNodes(): Long = {
    nodeCount
  }

  def size(): Long = {
    count()
  }

  def IntervalTree(): Unit = {
    root = null
  }

  def insert(r: Array[Long],t:Array[Long], v: T): Boolean = {
    insert(r,t, Iterator(v))
  }

  def insert(r: Array[Long],t:Array[Long], vs: Iterator[T]): Boolean = {
    insertInterval(r,t, vs)
    if (Math.abs(leftDepth - rightDepth) > threshold) {
      rebalance()
    }
    true
  }

  /*
  * This method finds an existing node (keyed by Interval) to insert the data into,
  * or creates a new node to insert it into the tree
  */
  private def insertInterval(value:Array[Long],time:Array[Long], vs: Iterator[T]) = {
    if (root == null) {
      nodeCount += 1
      root = new Node[T](value,time)
      root.multiput(vs)
    }
    else {
     // println(vs.toString())
     // vs.map(iter => iter.toString).foreach(println(_))
      var curr: Node[T] = root
      var parent: Node[T] = null
      var search: Boolean = true
      var leftSide: Boolean = false
      var rightSide: Boolean = false
      var tempLeftDepth: Long = 0
      var tempRightDepth: Long = 0

      while (search) {
        curr.NodeMax=math.max(curr.NodeMax,time(1))
        curr.NodeMin=math.min(curr.NodeMin,time(0))
        parent = curr
        if (curr.BiggerThan(value(1))) {
          //left traversal
          if (!leftSide && !rightSide) {
            leftSide = true
          }
          tempLeftDepth += 1
          curr = curr.leftChild
          if (curr == null) {
            curr = new Node(value, time)
            curr.multiput(vs)
            parent.leftChild = curr
            nodeCount += 1
            search = false
          }
        } else if (curr.SmallerThan(value(0))) {
          //right traversal
          if (!leftSide && !rightSide) {
            rightSide = true
          }
          tempRightDepth += 1
          curr = curr.rightChild
          if (curr == null) {
            curr = new Node(value, time)
            curr.multiput(vs)
            parent.rightChild = curr
            nodeCount += 1
            search = false
          }
        } else {
          // insert new id, given id is not in tree
          curr.multiput(vs)
          search = false
        }
      }
      // done searching, set our max depths
      if (tempLeftDepth > leftDepth) {
        leftDepth = tempLeftDepth
      } else if (tempRightDepth > rightDepth) {
        rightDepth = tempRightDepth
      }
    }
  }

  /* searches for single interval over single id */
  def search(r:Long): Iterator[T] = {
    search(r, root)
  }


  def search(timepoint:Long,n:Node[T]):Iterator[T] ={

    val results = new ListBuffer[T]()

    if(n!=null)
      {
        if(n.NodeMax<timepoint)
          {
            results++=n.get()
            results++=n.get()
            results++=n.get()
            return  results.toIterator
          }else if(n.NodeMin>timepoint)
          {
            results++=n.get()
            results++=n.get()
            return results.toIterator
          }


        if(timepoint>=n.minTime&&timepoint<=n.maxTime)
          n.get()
        else if(timepoint<n.minTime)
          search(timepoint, n.leftChild)
        else
          search(timepoint,n.rightChild)
      }
    else
      {
        null
      }
    //return results.distinct.toIterator
   /* val results=new ListBuffer[T]()

    if(root.leftChild!=null)
      {
        if(timepoint<=root.midTime)
          results ++= search(timepoint,root.leftChild)
        else if(root.rightChild!=null)
          results ++= search(timepoint,root.rightChild)
        else
          results ++ root.get()
      }
    else
      results++=root.get()
    results.toIterable*/
  }

  // currently gets an inorder list of the tree, then bulk constructs a new tree
  private def rebalance(): Unit = {
    val nodes: List[Node[T]] = inOrder()
    root = null
    nodeCount = 0
    val orderedList = nodes.sortWith(_.minTime < _.minTime)
    orderedList.foreach(n => n.clearChildren())
    insertRecursive(orderedList)
  }
  private def insertRecursive(nodes: List[Node[T]]): Unit = {
    if (nodes == null) {
      return
    }
    if (nodes.nonEmpty) {
      val count = nodes.length
      val middle = count/2
      val node = nodes(middle)

      insertNode(node)
      insertRecursive(nodes.take(middle))
      insertRecursive(nodes.drop(middle + 1))
    }
  }

  def insertNode(n: Node[T]): Boolean = {
    if (root == null) {
      root = n
      nodeCount += 1
      return true
    }
    var curr: Node[T] = root
    var parent: Node[T] = null
    var search: Boolean = true
    var leftSide: Boolean = false
    var rightSide: Boolean = false
    var tempLeftDepth: Long = 0
    var tempRightDepth: Long = 0
    while (search) {
      curr.NodeMax = Math.max(curr.NodeMax, n.maxTime)
      curr.NodeMin=math.min(curr.NodeMin,n.NodeMin)
      parent = curr
      if (curr.BiggerThan(n.maxTime)) { //left traversal
        if (!leftSide && !rightSide) {
          leftSide = true
        }
        tempLeftDepth += 1
        curr = curr.leftChild
        if (curr == null) {
          parent.leftChild = n
          nodeCount += 1
          search = false
        }
      } else if (curr.SmallerThan(n.minTime)) { //right traversal
        if (!leftSide && !rightSide) {
          rightSide = true
        }
        tempRightDepth += 1
        curr = curr.rightChild
        if (curr == null) {
          parent.rightChild= n
          nodeCount += 1
          search = false
        }
      } else { // attempting to replace a node already in tree. Merge
        curr.multiput(n.get())
        search = false
      }
    }
    // done searching, now let's set our max depths
    if (tempLeftDepth > leftDepth) {
      leftDepth = tempLeftDepth
    } else if (tempRightDepth > rightDepth) {
      rightDepth = tempRightDepth
    }
    true
  }


  private def inOrder(): List[Node[T]] = {
    inOrder(root).toList
  }

  private def count(): Long = {
    count(root)
  }

  private def count(n: Node[T]): Long = {
    var total: Long = 0
    if (n == null) {
      return total
    }
    total += n.getSize
    total += count(n.leftChild)
    total += count(n.rightChild)
    total
  }

  private def inOrder(n: Node[T]): ListBuffer[Node[T]] = {
    val seen = new ListBuffer[Node[T]]()
    if (n == null) {
      return seen
    }
    seen += n.clone
    seen ++= inOrder(n.leftChild)
    seen ++= inOrder(n.rightChild)
    seen
  }

}
