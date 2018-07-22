package org.apache.spark.sql.timo.execution.TopK

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.timo.TemporalRDD
import org.apache.spark.sql.timo.execution.TimoPlan
import org.apache.spark.sql.timo.index.{IndexedPartition, STEIDIndex}
import org.apache.spark.sql.timo.temporal.MinHeap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}


case class SlotTopk(child:SparkPlan, columnKey:Seq[Expression], starttime:Literal,
                    endtime:Literal, k:Literal,flag:Literal) extends TimoPlan {

  override def output = child.output

  override def children: Seq[SparkPlan] = Seq(child)

  def getPartition(first:Long,second:Long,bounds:Array[(Long,Long)]): mutable.HashSet[Int] ={

    val partition_sets = new mutable.HashSet[Int]

    var start=bounds.indexWhere(ele=>(ele._1<=first&&ele._2>=first))
    var end =bounds.indexWhere(ele=>(ele._2>=second&&ele._1<=second))
    if(end == -1)
      end=bounds.length
    if(start>=0){
      while(start<=end)
      {
        partition_sets.add(start)
        start+=1
      }
    }

    partition_sets
  }

  override protected def doExecute(): RDD[InternalRow] = {

    val data=child.execute().asInstanceOf[TemporalRDD[(Long,Long)]]

    var query_time:Array[Long]=new Array[Long](2)
    query_time(0)=starttime.value.asInstanceOf[Long]
    query_time(1)=endtime.value.asInstanceOf[Long]
    var query_sets:mutable.HashSet[Int] = new mutable.HashSet[Int]
    query_sets=getPartition(query_time(0),query_time(1),data.bounds)

    val prund=new PartitionPruningRDD[IndexedPartition](data.temporalPartition,query_sets.contains)

    val after=prund.flatMap(packed => {

      val data = packed.data
      val index=packed.index.asInstanceOf[STEIDIndex[Int]]
      val min_time=index.mintime
      val max_time=index.maxtime
      var mid_partition_flag=false
      var basetime=index.basetime

      var result:ArrayBuffer[Int]=new ArrayBuffer[Int]()
      if(query_time(0)>=min_time && query_time(1)<=max_time) {
        result = index.get_ST_Result_Topk((query_time(0)-basetime).toInt, (query_time(1)-basetime).toInt)
      }else if(query_time(0)<min_time && query_time(1)>max_time){
        mid_partition_flag=true
      }else if(query_time(0)<max_time &&query_time(0)>=min_time &&query_time(1)>max_time){
        result=index.get_ST_Result_Topk((query_time(0)-basetime).toInt, (max_time-basetime).toInt)
      }else{
        result=index.get_ST_Result_Topk((min_time-basetime).toInt, (query_time(1)-basetime).toInt)
      }

      if(mid_partition_flag==true){
        data
      }else {
        result.distinct.map(iter => packed.data(iter))
      }
    })

    val topK=after.map(iter=>(iter.getInt(1))).collect()

    val test=topK.groupBy(iter=>iter)
    val top_test=test.map(iter=>iter._2.length).toArray

    val minheap = new MinHeap()

    minheap.apply(k.value.asInstanceOf[Int])
    minheap.build(top_test)

    var topk = minheap.TopK().filter(_ != 0)
    var final_result=test.filter(iter=>topk.contains(iter._2.length)).map(iter=>iter._1).toArray

    val b=sparkContext.broadcast(final_result)

    after.mapPartitions(iter=>{
      val data=iter.toArray
      var broaddata=b.value

      var result1:ArrayBuffer[InternalRow]=new ArrayBuffer[InternalRow]()
      breakable {
        for (i <- 0 to data.length - 1) {
          if (broaddata.contains(data(i).getInt(1))) {
            broaddata = broaddata.filter(_ != data(i).getInt(1))
            result1+=data(i)
          }
        }
        if(broaddata.length==0){
          break()
        }
      }

      result1.toIterator
    })

  }
}
