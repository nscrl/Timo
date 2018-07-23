package org.apache.spark.sql.timo

import java.beans.Introspector

import com.sun.tools.javac.code.TypeTag
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, JavaTypeInference}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.{Encoder, Row, SQLContext, SparkSession, DataFrame => SQLDataFrame, Dataset => SQLDataset}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{ExternalRDD, LogicalRDD, PlanLater}
import org.apache.spark.sql.timo.execution.QueryExecution
import org.apache.spark.sql.timo.expression._
import org.apache.spark.sql.timo.index.{HashType, IndexType}
import org.apache.spark.sql.timo.util.LiteralUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils



class Dataset[T] private[timo](@transient val TimoSession: TimoSession,
                               @transient override val queryExecution: QueryExecution,
                               encoder: Encoder[T])
  extends SQLDataset[T](TimoSession, queryExecution.logical, encoder) {self =>

  val attrs:Seq[Attribute] = this.queryExecution.analyzed.output

  def this(TimoSession: TimoSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(TimoSession, {
      val qe = TimoSession.sessionState.executePlan(logicalPlan)
      qe
    }, encoder)
  }

  ////////////////////////////////////////
  //Search operations
  ////////////////////////////////////////
  def Range_Find(records:String,start_point:Long,end_point:Long):DataFrame=withPlan {

    val attrs = getAttributes(Array(records))
    attrs.foreach(attr => assert(attr != null, "column not found"))
    Filter(InRange(TimeWrapper(attrs)
          ,LiteralUtil(start_point), LiteralUtil(end_point))
          ,logicalPlan)
  }

  def Interval_Find(records:Array[String],start_point:Long,end_point:Long):DataFrame =withPlan{
    val attrs=getAttributes(records)
    attrs.foreach(attr=>assert(attr!=null , "column not found"))
    Filter(IntervalFind(TimeWrapper(attrs)
          ,LiteralUtil(start_point),LiteralUtil(end_point),LiteralUtil(0))
          ,logicalPlan)
  }

  def Temporal_Find(records:Array[String],timepoint:Long): DataFrame =withPlan{
    val attrs=getAttributes(records)
    attrs.foreach(attr=>assert(attr!=null,"column not found"))
    Filter(InTemporal(TimeWrapper(attrs)
          ,LiteralUtil(timepoint))
          ,logicalPlan)
  }

  ////////////////////////////////////////
  //Aggregation operations
  ////////////////////////////////////////
  def Max(cols:String,timepoint:Long,timepoint1:Long):DataFrame=withPlan{
    val attr=getAttributes(Array(cols))
    TimoSession.sessionState.setConf("timo.aggerator.order",attrs.indexWhere(_.name == cols).toString)
    attr.foreach(attr=>assert(attr!=null,"Column not found"))
    Filter(InMax(TimeWrapper(attr)
          ,LiteralUtil(timepoint),LiteralUtil(timepoint1),LiteralUtil(1))
          ,logicalPlan)
  }

  def Min(cols:String,timepoint:Long,timepoint1:Long):DataFrame=withPlan{
    val attr=getAttributes(Array(cols))
    TimoSession.sessionState.setConf("timo.aggerator.order",attrs.indexWhere(_.name == cols).toString)
    attr.foreach(attr=>assert(attr!=null,"Column not found"))
    Filter(InMin(TimeWrapper(attr)
          ,LiteralUtil(timepoint),LiteralUtil(timepoint1),LiteralUtil(2))
          ,logicalPlan)
  }

  def Mean(cols:String,timepoint:Long,timepoint1:Long):DataFrame=withPlan{
    val attr=getAttributes(Array(cols))
    TimoSession.sessionState.setConf("timo.aggerator.order",attrs.indexWhere(_.name == cols).toString)
    attr.foreach(attr=>assert(attr!=null,"Column not found"))
    Filter(InMean(TimeWrapper(attr)
          ,LiteralUtil(timepoint),LiteralUtil(timepoint1),LiteralUtil(3))
          ,logicalPlan)
  }

  ////////////////////////////////////////
  //Top-K operations
  ////////////////////////////////////////

  def slotTopk(key:String,startpoint:Long,endpoint:Long,k:Int) : DataFrame = withPlan {
    val attr=getAttributes(Array(key))

    Filter(InTopK(TimeWrapper(attr),
      LiteralUtil(startpoint),
      LiteralUtil(endpoint),LiteralUtil(k),LiteralUtil(1))
      ,logicalPlan)
  }

  def TopK(records:Array[String],timepoint:Long,timepoint1:Long,k:Int): DataFrame =withPlan{
    val attrs = getAttributes(records)
    attrs.foreach(attr => assert(attr != null, "column not found"))
    Filter(InTopK(TimeWrapper(attrs)
      ,LiteralUtil(timepoint),LiteralUtil(timepoint1),LiteralUtil(k),LiteralUtil(1))
      ,logicalPlan)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Index operations
  /////////////////////////////////////////////////////////////////////////////

  def index(indexType: IndexType, indexName: String, column: Array[String],period:String): this.type = {

    val periodNum=getPeriod(period)
    TimoSession.sessionState.setConf("timo.partition.period",periodNum.toString)
    TimoSession.sessionState.indexManager.createIndexQuery(this, indexType, indexName, getAttributes(column).toList)
    this
  }

  private def getPeriod(period:String):Long={
    period.toUpperCase match{
      case "YEAR"   =>    10000000000L
      case "MONTH"  =>    100000000L
      case "DAY"    =>    1000000L
      case "HOUR"   =>    10000L
      case "MINUTE" =>    100L
    }
  }

  /**
    * @group extended
    */
  def setStorageLevel(indexName: String, level: StorageLevel): this.type = {
    TimoSession.sessionState.indexManager.setStorageLevel(this, indexName, level)
    this
  }

  private def getAttributes(keys: Array[String], attrs: Seq[Attribute] = this.queryExecution.analyzed.output)
  : Array[Attribute] = {

    keys.map(key => {
      val temp = attrs.indexWhere(_.name == key)
      if (temp >= 0) attrs(temp)
      else null
    })
  }

  @inline private def withPlan(logicalPlan: => LogicalPlan): DataFrame = {
    Dataset.ofRows(TimoSession, logicalPlan)
  }
}

private[timo] object Dataset {

  def apply[T: Encoder](TimoSession: TimoSession, logicalPlan: LogicalPlan): Dataset[T] = {
    new Dataset(TimoSession, logicalPlan, implicitly[Encoder[T]])
  }

  def ofRows(TimoSession: TimoSession, logicalPlan: LogicalPlan): Dataset[Row] = {
    val qe = TimoSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](TimoSession, qe, RowEncoder(qe.analyzed.schema))
  }
}