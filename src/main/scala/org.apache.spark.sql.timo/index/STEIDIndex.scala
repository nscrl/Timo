package org.apache.spark.sql.timo.index


import java.io._

import util.control.Breaks._
import java.util
import java.io.BufferedWriter
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.timo.TimoConf
import org.apache.zookeeper.server.SessionTracker.Session

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class STEIDIndex[T] extends Index with Serializable{

  var ST=new util.HashMap[Int,Array[Int]]()
  var ED=new util.HashMap[Int,Array[Int]]()
  var ID=new util.HashMap[Int,Array[Int]]()

  //false represents single and true is double
  var single_double=false

  var basetime=0L
  var bounds:ArrayBuffer[Long]=new ArrayBuffer[Long]()
  var num=0L
  var mintime=0L
  var maxtime=0L
  var count=0

  def getResult(start:Long,end:Long): ArrayBuffer[Int] ={

    if(start==end){

      get_ST_Result(start-basetime,end-basetime)
    }
    else
    {
      var S_Offset = 0L
      var E_Offset = 0L

      //if only a bound
      if (start < mintime) {
        S_Offset = 0L
      } else {
        S_Offset = start - basetime
      }

      if (end > maxtime) {
        E_Offset = ED.values().size().toLong
      } else {
        E_Offset = end - basetime
      }

      (get_ST_Result(S_Offset, E_Offset) ++ get_ID_Result(S_Offset, E_Offset)).distinct
    }
  }

  def get_ST_Result(start:Long,end:Long): ArrayBuffer[Int] ={

    val result=new ArrayBuffer[Int]()

    if(start==end){
      result ++=ST.get(start)
    }else {
      for (i:Long <- start until end) {
        if(ST.containsKey(i)) {
          result ++= ST.get(i)
        }
      }
    }
    result
  }

  def get_ID_Result(start:Long,end:Long): ArrayBuffer[Int] ={

    var bound=bounds.indexWhere(iter=>iter>start)-1
    var id_Result=ID.get(bound*num)
    var st_Result=ST.get(bound*num)
    var ed=get_ED_Result(bound*num,start)
    var i=bound.toLong*num
    var result=new ArrayBuffer[Int]()

    if(i!=0){
      if(ID.containsKey(i)) {
        result ++= ID.get(i)
      }
    }

    while(i<start){
      if(ST.containsKey(i)) {
        result ++= ST.get(i)
      }
      i+=1
    }
    result.filter(iter=>FilterArray(iter,ed))
  }

  def get_ED_Result(temporal:Long,end:Long): ArrayBuffer[Int] ={

    val result=new ArrayBuffer[Int]()
    var start=0L
    var i=temporal
    while(i<end){
      if(ED.containsKey(i))
        if(i!=0) {
          result ++= ED.get(i)
        }
      i+=1
    }
    result
  }

  def get_ST_Result_Topk(start:Int,end:Int): ArrayBuffer[Int] ={

    val result=new ArrayBuffer[Int]()
    var stime=0L
    var etime=0L
    var all_sum=0L
    if(start==end){

      all_sum+=etime-stime
      result ++=ST.get(start)
    }else {
      for (i:Int <- start until end) {
        if(ST.containsKey(i)) {

          result ++= ST.get(i)
        }
      }
    }
    result
  }

  def FilterArray(compare:Int,compared:ArrayBuffer[Int]):Boolean={

    for(i<-0 to compared.length-1)
    {
      if(compare==compared(i))
        return false
    }
    return true
  }

}

object STEIDIndex{

  def apply[T](data:Array[InternalRow]):STEIDIndex[T] = {

    val index=new STEIDIndex[T]()

    val min=data.sortBy(_.getLong(1)).apply(0).getLong(1)
    val max=data.sortBy(_.getLong(2)).map(iter=>iter.getLong(2)).max
    index.basetime=min
    index.mintime=min
    index.maxtime=max
    if(TimoConf.INDEX_ATTR_NUM==1) {
      index.single_double=false
    }else{
      index.single_double=true
    }

    var delta=0
    var end=0L
    var include=Array[Long]()

    var num=(calcu_internal_time(data)*1).toLong

    index.num=num.toInt

    if(num!=0) {
      for (i <- 0 until data.length) {

        if (index.single_double == false) {
          delta = (data(i).getLong(1) - index.basetime).toInt
          if (index.ED.containsKey(delta)) {
            index.ED.replace(delta, index.ED.get(delta) ++ Array(i))
          } else {
            index.ED.put(delta, Array(i))
          }
        } else {
          delta = (data(i).getLong(1) - index.basetime).toInt
          val interval_end=data(i).getLong(2)

          end = (interval_end - index.basetime).toInt
          if (index.ST.containsKey(delta)) {
            index.ST.replace(delta, index.ST.get(delta) ++ Array(i))
          } else {
            index.ST.put(delta, Array(i))
          }

          if (index.ED.containsKey(end.toInt)) {
            try {
              index.ED.replace(end.toInt, index.ED.get(end) ++ Array(i))
            }catch{
              case e:NullPointerException => println("debug")
            }
          } else {
            index.ED.put(end.toInt, Array(i))
          }

          if (i == 0) {
            index.ID.put(0, null)
            index.ED.put(0, null)
          }
          else if (((end / num) > (delta / num))&&((delta+1)/num!=0)) {
            var j = (delta + 1) / num
            while (j <= (end - 1) / num) {
              if (index.ID.containsKey(j * num)) {
                index.ID.replace((j * num).toInt, index.ID.get(j * num) ++ Array(i))
              } else {
                index.ID.put((j * num).toInt, Array(i))
              }
              j += 1
            }
          }
        }
      }
    }

    for(j<-0 to index.ID.size()){
      index.bounds.append(j*num)
    }
    index
  }

  def calcu_internal_time(data:Array[InternalRow]):Int={

    var sum=0L
    val length=data.length

    data.foreach(iter=>{
      sum+=(iter.getLong(2)-iter.getLong(1))
    })

    (sum/length).toInt
  }
}