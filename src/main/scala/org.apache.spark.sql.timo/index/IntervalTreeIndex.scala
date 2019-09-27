package org.apache.spark.sql.timo.index

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.timo.temporal.IntervalTree

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by Elroy on 5/15/2017.
  */

class IntervalTreeIndex[T] extends Index {
  var index = new IntervalTree[Long]()
  var size = 0

  var bounds = ArrayBuffer.empty[Int]
  var result:Array[Int]=null
  var Sorted:List[Int]=null
  var original:Array[Long]=null
}

object IntervalTreeIndex extends Serializable {

  def apply[T](data: Array[(T, InternalRow)]) = {
    val ans = new IntervalTreeIndex[T]
    var minTime = data(0)._1.toString.toLong
    var res:Array[Long]=Array()
    val zz=new util.HashMap[Int,Array[Int]]()

    val slen = 1000L                                                      //每条记录的大小值
    val maxTime = data(data.length-1)._1.toString.toLong
    val size = math.sqrt((maxTime + 1L - minTime)).toInt           //获得叶子节点数
    //  val len = size  * slen                                                //
    val len=(data.length/size).toLong
    var ref = 0

    var minValue=0L
    var maxValue=0L
    for (i <- 1 until size){

      minTime = minTime + len
      ans.bounds += ref
      minValue=data(ref)._1.toString.toLong
      val attr=data(ref)._2.getLong(2)
      breakable {
        while (ref < data.length) {
          if (data(ref)._1.toString.toLong > minTime - 1) {
            break
          }

          ref += 1
        }
      }
      maxValue=data(ref-1)._1.toString().toLong
      ans.index.insert(Array(minValue,maxValue),Array(minTime-len,minTime-1),i.toLong)
    }
    if (minTime < maxTime){
      ans.bounds += ref
      ans.size = size
      minValue=data(ref)._1.toString.toLong
      maxValue=data(data.length-1)._1.toString.toLong
      ans.index.insert(Array(minValue,maxValue),Array(minTime,minTime+len-1),size.toLong)
    }
    ans
  }
}
