package org.apache.spark.sql.timo.temporal

/**
  * Created by mint on 17-5-29.
  */
class MinHeap {

  var data:Array[Int]=null

  def apply(K:Int){
    data=new Array[Int](K+1)
    for(i<-K to 1){
      data(i)=i
    }
    build(K)
  }

  def build(k:Int): Unit ={
    var pos=k/2
    for(i<-pos to 1)
      UpToDown(i)
  }
  def UpToDown(i:Int): Unit ={
    var t1=2*i
    var t2=t1+1
    var tmp=0
    var pos=0

    if(t1>data.length-1)
      return
    else{
      if(t2>data.length-1)
        pos=t1
      else{
        if (data(t1) > data(t2)) pos = t2
        else pos=t1
      }
      if(data(i)>data(pos))
        {
         tmp=data(i)
          data(i)=data(pos)
          data(pos)=tmp
          UpToDown(pos)
        }
    }
  }

  def TopK():Array[Int]={
    data
  }

  def build(insertdata:Array[Int]): Unit ={
    for(i<-0 to insertdata.length-1)
      if(insertdata(i)>data(1))
        {
          data(1)=insertdata(i)
          UpToDown(1)
        }
  }
}