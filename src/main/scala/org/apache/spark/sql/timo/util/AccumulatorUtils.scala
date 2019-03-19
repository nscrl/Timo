package org.apache.spark.sql.timo.util

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


class AccumulatorUtils {

}

class MapAccumulator extends AccumulatorV2[(String,Int),mutable.Map[String,Int]] with Serializable{
  val mapAccumulator: mutable.Map[String, Int] =mutable.Map[String,Int]()
  def add(keyAndvalue:(String,Int)): Unit ={
    val key=keyAndvalue._1
    val value=keyAndvalue._2
    if(!mapAccumulator.contains(key))
      mapAccumulator+=key->value
    else
      mapAccumulator+=key->(mapAccumulator(key)+value)
  }

  def add1(keyAndValue:(Long,Int)): Unit ={
    val key=keyAndValue._1.toString
    val value=keyAndValue._2
    mapAccumulator+=key->value

  }
  def getValue(i:Int): Array[String] ={
    val zz: Array[String] = new Array[String](mapAccumulator.values.size)
    var j=0
    mapAccumulator.foreach(iter=>
    if(iter._2==i) {
      zz(j) = iter._1
      j = j + 1
    }
    )
    zz
  }

  def delete(value:Long): Unit ={
    if(mapAccumulator.contains(value.toString)) {
      mapAccumulator -= value.toString
    }
  }

  def isZero:Boolean={
    mapAccumulator.isEmpty
  }

  def copy():AccumulatorV2[(String,Int),mutable.Map[String,Int]]={
    val newMapAccumulator=new MapAccumulator()
    mapAccumulator.foreach(iter=>newMapAccumulator.add(iter))
    newMapAccumulator
  }

  def value:mutable.Map[String,Int]={
    mapAccumulator
  }
  def merge(other:AccumulatorV2[(String,Int),mutable.Map[String,Int]]): Unit =other match
  {
    case _:MapAccumulator=>
      other.value.foreach(x=>
      if(!this.value.contains(x._1))
        this.add(x)
      )
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}"
      )
  }
  def reset(): Unit ={
    mapAccumulator.clear()
  }
}

class AccumulatorLong extends AccumulatorV2[Array[Long],Array[Long]] with Serializable{
  @transient
  var LongArray:Array[Long]=new Array[Long](20)
  @transient
  var i:Int=0

  override def copyAndReset(): AccumulatorV2[Array[Long], Array[Long]] = super.copyAndReset()

  def add(value:Array[Long]):Unit={
    LongArray(LongArray.length)=value(0)
  }

  def contains(num:Long):Boolean={
    for(i<-0 to 9)
      {
        if(LongArray(i)==num)
          return true
      }
    false
  }

  def add(value:Long): Unit ={
    LongArray(i)=value
    i+=1
  }

  def delet(num:Long): Unit ={
    for(i<-0 to 20)
      if(LongArray(i)==num)
        LongArray(i)=0
  }

  def getValue(i:Int):Long={
    LongArray(i)
  }

  override def isZero: Boolean = {
    LongArray=new Array[Long](20)
    true
  }

  override def copy(): AccumulatorLong = {
    val newLong=new AccumulatorLong()
    LongArray.foreach(iter=>newLong.add(iter))
    newLong

  }

  override def value: Array[Long] = {
    LongArray
  }

  override def merge(other: AccumulatorV2[Array[Long], Array[Long]]): Unit =other match {
    case _:AccumulatorLong=>
      other.value.foreach(x=>
        if(!this.value.contains(x))
          this.add(x)
      )
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}"
      )
  }

 override def reset(): Unit ={

    LongArray.map(iter=>iter)

  }



}