package org.apache.spark.sql.timo.index

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.timo.TemporalRDD
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
                          indexName: String)(var _indexedRDD: RDD[IndexedPartition] = null,
                                             var range_bounds: Array[Long] = null)
  extends IndexedRelation with MultiInstanceRelation {
  require(columnKeys.length == 1)

  val attr_order=timoSession.sessionState.TimoConf.aggeratorOrder.toInt

  var temporalRDD:TemporalRDD[Long]=buildIndex()

  private[timo] def buildIndex() : TemporalRDD[Long] = {
    val dataRDD = child.execute().map(row => {
      val key = BindReferences
        .bindReference(columnKeys(0), child.output)
        .eval(row)
        .asInstanceOf[Number].longValue()
      (key, row)
    })
    val numShufflePartitions = timoSession.sessionState.TimoConf.indexPartitions
    val (partitionedRDD, tmp_bounds) = RangePartition(dataRDD, numShufflePartitions)
    range_bounds=tmp_bounds
    val indexed=partitionedRDD.mapPartitions(iter=>{
      val data=iter.toArray
      val index=HashIndex(data)

      Array(IndexedPartition(data.map(_._2),index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    indexed.setName(tableName.map(n => s"$n $indexName").getOrElse(child.toString))
    new TemporalRDD[Long](indexed,range_bounds)
  }

  override def newInstance() = {
    new HashIndexRelation(output.map(_.newInstance()), child, tableName, columnKeys, indexName)(_indexedRDD)
  }

  override def withOutput(newOutput: Seq[Attribute]): IndexedRelation = {
    //simbasessionstate/indexedrelationscan 下面的sparkplan 详细见indexedrelation的定义
    HashIndexRelation(newOutput, child, tableName, columnKeys, indexName)(_indexedRDD,range_bounds)
  }
}