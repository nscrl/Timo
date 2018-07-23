package org.apache.spark.sql.timo.index

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.timo.{TemporalRdd}
import org.apache.spark.sql.timo.partitioner.RangePartition
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

/**
  * Created by houkailiu on 2017/7/8.
  */
case class HashIndexRelation(
                          output: Seq[Attribute],
                          child: SparkPlan,
                          tableName: Option[String],
                          columnKeys: List[Attribute],
                          indexName: String)(var temporalRDD: TemporalRDD=null)
  extends IndexedRelation with MultiInstanceRelation {

  val attr_order=timoSession.sessionState.TimoConf.aggeratorOrder.toInt

  if(temporalRDD==null){
    buildIndex()
  }

  private[timo] def buildIndex() = {
    val dataRDD = child.execute().map(row => {
      val key = BindReferences
        .bindReference(columnKeys(0), child.output)
        .eval(row)
        .asInstanceOf[Number].longValue()
      (key, row)
    })
    val numShufflePartitions = timoSession.sessionState.TimoConf.indexPartitions
    val (partitionedRDD, tmp_bounds) = RangePartition(dataRDD, numShufflePartitions)
    val indexed=partitionedRDD.mapPartitions(iter=>{
      val data=iter.toArray
      val index=HashIndex(data)

      Array(IndexedPartition(data.map(_._2),index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    indexed.setName(tableName.map(n => s"$n $indexName").getOrElse(child.toString))
    temporalRDD=new TemporalRdd[Long](indexed,tmp_bounds)
  }

  override def newInstance() = {
    new HashIndexRelation(output.map(_.newInstance()), child, tableName, columnKeys, indexName)(temporalRDD)
  }

  override def withOutput(newOutput: Seq[Attribute]): IndexedRelation = {
    //simbasessionstate/indexedrelationscan 下面的sparkplan 详细见indexedrelation的定义
    HashIndexRelation(newOutput, child, tableName, columnKeys, indexName)(temporalRDD)
  }
}