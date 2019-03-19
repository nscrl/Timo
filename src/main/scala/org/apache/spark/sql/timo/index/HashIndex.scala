package org.apache.spark.sql.timo.index

import org.apache.spark.sql.catalyst.InternalRow

import scala.util.control.Breaks.breakable

/**
  * Created by houkailiu on 2017/7/8.
  */
class HashIndex extends Index with Serializable{
  var basetime=0L
  var index = new java.util.HashMap[Int, Array[Int]]()
}

object HashIndex {

  def apply[T](data: Array[(T, InternalRow)]): HashIndex = {

    var attr_order=1//SASConf.AGGERATOR_ORDER.toString.toInt
    attr_order=1
    val ans = new HashIndex
    val minTime = data(0)._1.toString.toLong
    ans.basetime=minTime

    var remainder=0
    var ref=0
    while (ref < data.length) {
      breakable {
        remainder=(data(ref)._1.toString.toLong-minTime).toInt
        if(ans.index.containsKey(remainder))
        {
          ans.index.replace(remainder,ans.index.get(remainder),ans.index.get(remainder)++Array(ref))
        }
        else
        {
          ans.index.put(remainder,Array(ref))
        }
        ref += 1
      }
    }
    ans
  }

}