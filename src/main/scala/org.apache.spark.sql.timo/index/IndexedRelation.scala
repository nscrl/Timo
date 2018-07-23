package org.apache.spark.sql.timo.index

import org.apache.spark.sql.timo.TimoSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by mint on 5/15/16.
  * Indexed Relation Structures for T-QAS
  */

private[timo] case class IndexedPartition(data: Array[InternalRow], index: Index)
private[timo] case class DPartition(data:Array[InternalRow])
private[timo] object IndexedRelation {
  def apply(child: SparkPlan, table_name: Option[String], index_type: IndexType,
            column_keys: List[Attribute], index_name: String): IndexedRelation = {
    index_type match {

      case HashType=>
        HashIndexRelation(child.output,child,table_name,column_keys,index_name)()

      case STEIDType=>
        STEIDIndexRelation(child.output,child,table_name,column_keys,index_name)()

      case _ => null
    }
  }
}

private[timo] abstract class IndexedRelation extends LogicalPlan {
  self: Product =>

  var temporalRDD:TemporalRDD
  def timoSession = TimoSession.getActiveSession.orNull

  override def children: Seq[LogicalPlan] = Nil
  def output: Seq[Attribute]

  def withOutput(newOutput: Seq[Attribute]): IndexedRelation
}