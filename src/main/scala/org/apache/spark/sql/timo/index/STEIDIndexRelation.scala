package org.apache.spark.sql.timo.index


import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.timo.TemporalRDD
import org.apache.spark.sql.timo.partitioner.TemporalPartitioner
import org.apache.spark.storage.StorageLevel

case class STEIDIndexRelation (
                                output:Seq[Attribute],
                                child:SparkPlan,
                                tableName:Option[String],
                                columnKeys:List[Attribute],
                                indexName:String)(var temporalRDD: TemporalRDD[Long] = null)
extends IndexedRelation with MultiInstanceRelation{


  if(temporalRDD==null){
    buildIndex()
  }

  private def buildIndex(): Unit ={

    val dataRDD = child.execute().map(row => {
      val ed = BindReferences
        .bindReference(columnKeys(1), child.output)
        .eval(row)
        .asInstanceOf[Number].longValue()
      (ed, row)
    })

    val period=timoSession.sessionState.getConf("timo.partition.period").toLong
    val partition_num=timoSession.sessionState.getConf("timo.index.partitions").toInt
    val (partitionedRDD,tmp_bounds, _)=TemporalPartitioner(dataRDD,period,partition_num)

    val indexed=partitionedRDD.mapPartitions(iter=>{
      val data=iter.toArray
      val index=STEIDIndex[Int](data)

      Array(IndexedPartition(data,index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    indexed.setName(tableName.map(n=>s"$n $indexName").getOrElse(child.toString()))
    temporalRDD=new TemporalRDD[Long](indexed,tmp_bounds)
  }

  override def  newInstance(): STEIDIndexRelation.this.type ={
    STEIDIndexRelation(output.map(_.newInstance()), child, tableName, columnKeys, indexName)(temporalRDD).asInstanceOf[this.type]
  }

  override def withOutput(newOutput: Seq[Attribute]): IndexedRelation = {
    STEIDIndexRelation(newOutput,child,tableName,columnKeys,indexName)(temporalRDD)
  }
}