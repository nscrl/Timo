package org.apache.spark.sql.timo.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.timo.index.{IndexedPartition, IndexedRelation, IntervalTreeIndex}
import org.apache.spark.storage.StorageLevel

/**
  * Created by mint on 17-5-15.
  */
private[timo] case class IntervalTreeIndexRelation(
                                                   output: Seq[Attribute],
                                                   child: SparkPlan,
                                                   tableName: Option[String],
                                                   columnKeys: List[Attribute],
                                                   indexName: String)(var _indexedRDD: RDD[IndexedPartition] = null,
                                                                      var range_bounds: Array[Long] = null)
  extends IndexedRelation with MultiInstanceRelation {
  require(columnKeys.length >= 1)

  if (_indexedRDD == null) {

    buildIndex()
  }

  private[timo] def buildIndex() : Unit = {
    val dataRDD = child.execute().map(row => {
      val key1 = BindReferences
        .bindReference(columnKeys(0), child.output)
        .eval(row)
        .asInstanceOf[Number].longValue()
      (key1,row)
    })

    val numShufflePartitions = timoSession.sessionState.TimoConf.indexPartitions

    val (partitionedRDD, tmp_bounds) = RangePartition(dataRDD, numShufflePartitions)

    range_bounds = tmp_bounds

    val indexed = partitionedRDD.mapPartitions(iter => {
      val data = iter.toArray
      val index = IntervalTreeIndex(data)
      Array(IndexedPartition(data.map(_._2), index)).iterator
    }).persist(StorageLevel.MEMORY_ONLY)
    indexed.setName(tableName.map(n => s"$n $indexName").getOrElse(child.toString))
   //println("this is intervalTreeIndexRelation")
    _indexedRDD = indexed

  }

  override def newInstance() = {
    new IntervalTreeIndexRelation(output.map(_.newInstance()), child, tableName, columnKeys, indexName)(_indexedRDD).asInstanceOf[this.type]
  }

  override def withOutput(newOutput: Seq[Attribute]): IndexedRelation = {
    IntervalTreeIndexRelation(newOutput, child, tableName, columnKeys, indexName)(_indexedRDD, range_bounds)
  }
}
