
package org.apache.spark.sql.timo.index

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.timo.execution.TimoPlan
import org.apache.spark.sql.timo.expression._
import org.apache.spark.sql.timo.util.AccumulatorLong

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by houkailiu on 5/14/2017.
  */
private[timo] case class IndexedRelationScan(attributes: Seq[Attribute],
                                             predicates: Seq[Expression],
                                             relation: IndexedRelation)
  extends TimoPlan with PredicateHelper with Serializable{

  private val aggerator_order=TimoSessionState.TimoConf.aggeratorOrder
  private val order=TimoSessionState.TimoConf.getConfString("timo.aggerator.order").toInt

  override def children:Seq[SparkPlan] = Nil // for UnaryNode

  // All Filter functions
  def getExpression(condition: Expression, column: List[Attribute])
  : List[Expression] = {
    var ans:List[Expression] = List()
    condition.foreach {
      case now@InTime(_: Expression, _, _) =>
        ans = ans :+ now
      case now@InTemporal(_, _) =>
        ans = ans :+ now
      case now@InRange(_, _, _) =>
        ans = ans :+ now
      case now@IntervalFind(_, _, _, _) =>
        ans = ans :+ now
      case now@InMax(_, _, _, _) =>
        ans = ans :+ now
      case now@InMin(_, _, _, _) =>
        ans = ans :+ now
      case now@InMean(_, _, _, _) =>
        ans = ans :+ now
      case _ =>
        Nil
    }
    ans
  }

  override protected def doExecute(): RDD[InternalRow] = {

    val accmulator_long = new AccumulatorLong
    this.sparkContext.register(accmulator_long)

    def get_Partition(first:Long,second:Long,bounds:Array[Long]):mutable.HashSet[Int]={
      val query_sets = new mutable.HashSet[Int]

      var start=bounds.indexWhere(ele=>ele>=first)
      var end =bounds.indexWhere(ele=>ele>=second)

      if(end == -1)
        end=bounds.length
      if(start>=0){
        while(start<=end)
        {
          query_sets.add(start)
          start+=1
        }
      }
      query_sets
    }

    val after_filter = if (predicates.size == 1 && predicates.head.toString == "true"){
      relation.temporalRDD.temporalPartition.flatMap(_.data)
    } else
    relation match {
      // Hash Relation
      case hash @ HashIndexRelation(_,_,_,_,_)=>
        if(predicates.nonEmpty){
          predicates.map(predicates=>{
            var flag_agge=0

            val exps = getExpression(predicates, hash.columnKeys)
            val bounds=hash.temporalRDD.bounds
            val query_time:Array[Long]=new Array[Long](2)
            var ResultPartition=0
            var query_sets = new mutable.HashSet[Int]
            exps.foreach({
              case InTemporal(_:Expression,timepoint:Literal)=>
                query_time(0)=timepoint.value.asInstanceOf[Long]
                query_time(1)=query_time(0)
                ResultPartition=bounds.indexWhere(ele=>ele>=query_time(0))
                query_sets.add(ResultPartition)

              case InRange(_:Expression,st:Literal,ed:Literal)=>
                query_time(0)=st.value.asInstanceOf[Long]
                query_time(1)=ed.value.asInstanceOf[Long]
                query_sets=get_Partition(query_time(0),query_time(1),bounds)

              case InMax(_:Expression,st:Literal,ed:Literal,classify:Literal)=>
                flag_agge=classify.toString().toInt
                query_time(0)=st.value.asInstanceOf[Long]
                query_time(1)=ed.value.asInstanceOf[Long]
                query_sets=get_Partition(query_time(0),query_time(1),bounds)

              case InMin(_:Expression,st:Literal,ed:Literal,classify:Literal)=>
                flag_agge=classify.toString().toInt
                query_time(0)=st.value.asInstanceOf[Long]
                query_time(1)=ed.value.asInstanceOf[Long]
                query_sets=get_Partition(query_time(0),query_time(1),bounds)

              case InMean(_:Expression,st:Literal,ed:Literal,classify:Literal)=>
                flag_agge=classify.toString().toInt
                query_time(0)=st.value.asInstanceOf[Long]
                query_time(1)=ed.value.asInstanceOf[Long]
                query_sets=get_Partition(query_time(0),query_time(1),bounds)
            })

            val pruned=new PartitionPruningRDD[IndexedPartition](hash.temporalRDD.temporalPartition,query_sets.contains)

            val bd=sparkContext.broadcast(flag_agge)

            val res=pruned.flatMap(packed=>{
              val index=packed.index.asInstanceOf[HashIndex].index
              val basetime=packed.index.asInstanceOf[HashIndex].basetime
              val data=packed.data
              val flag_agge=bd.value
              var res:ArrayBuffer[Int]=new ArrayBuffer[Int]()
              val begin=query_time(0)-basetime
              val end=query_time(1)-basetime

              for(i<- begin.toInt to end.toInt){
                if(index.containsKey(i)){
                  res ++= index.get(i)
                }
              }
              val range_data=res.map(t=>data(t))
              if(flag_agge==1){

                Array(range_data.map(iter=>(iter.getLong(order),iter)).maxBy(_._1)._2).iterator
              } else if(flag_agge==2){

                Array(range_data.map(iter=>(iter.getLong(order),iter)).minBy(_._1)._2).iterator
              } else if(flag_agge==3){

                val All_Value = range_data.map(iter => iter.getLong(order))
                val Sum_value = All_Value.sum
                val Mean_value = Sum_value / range_data.length

                range_data.filter(iter=>iter.getLong(order) >= Mean_value)
              }else{
                range_data
              }
            })

            //to filter the max of every partitioner
            if(res.count()==1){
              res
            }else {
              if (flag_agge == 1) {
                val value = res.reduce((a, b) => if (a.getLong(order) > b.getLong(order)) a else b).getLong(order)
                res.filter(_.getLong(order) == value)
              } else if (flag_agge == 2) {
                val value = res.reduce((a, b) => if (a.getLong(order) < b.getLong(order)) a else b).getLong(order)
                res.filter(_.getLong(order) == value)
              } else if(flag_agge == 3){
                val All_Value = res.map(iter => iter.getLong(0))
                val Sum_value = All_Value.sum
                val Mean_value = Sum_value / res.count()
                res.filter(iter => iter.getLong(order) >= Mean_value)
              }else{
                res
              }
            }

          }).reduce((a,b)=>a.union(b))

        }
        else hash.temporalRDD.temporalPartition.flatMap(_.data)

      case steid @ STEIDIndexRelation(_,_,_,_,_)=>
        if(predicates.nonEmpty){
          predicates.map(predicates=>{

            val bounds=steid.temporalRDD.bounds

            val exps = getExpression(predicates, steid.columnKeys)
            val query_time: Array[Long] = new Array[Long](2)
            var query_sets = new mutable.HashSet[Int]
            var single_attr:Boolean=true

            var flag_agge=0
            exps.foreach({
              case InMax(_:Expression,st:Literal,ed:Literal,classify:Literal)=>
                flag_agge=classify.toString().toInt
                query_time(0)=st.value.asInstanceOf[Long]
                query_time(1)=ed.value.asInstanceOf[Long]
                query_sets=get_Partition(query_time(0),query_time(1),bounds)

              case InMin(_:Expression,st:Literal,ed:Literal,classify:Literal)=>
                flag_agge=classify.toString().toInt
                query_time(0)=st.value.asInstanceOf[Long]
                query_time(1)=ed.value.asInstanceOf[Long]
                query_sets=get_Partition(query_time(0),query_time(1),bounds)

              case InMean(_:Expression,st:Literal,ed:Literal,classify:Literal)=>
                flag_agge=classify.toString().toInt
                query_time(0)=st.value.asInstanceOf[Long]
                query_time(1)=ed.value.asInstanceOf[Long]
                query_sets=get_Partition(query_time(0),query_time(1),bounds)

              case InTemporal(_,temporal:Literal)=>
                query_time(0)=temporal.value.asInstanceOf[Long]
                query_time(1)=query_time(0)
                query_sets=get_Partition(query_time(0),query_time(1),bounds)

              case IntervalFind(_:Expression,st:Literal,ed:Literal,_)=>
                query_time(0)=st.value.asInstanceOf[Long]
                query_time(1)=ed.value.asInstanceOf[Long]
                query_sets=get_Partition(query_time(0),query_time(1),bounds)
                single_attr=false

              case InRange(_,st:Literal,ed:Literal)=>
                query_time(0)=st.value.asInstanceOf[Long]
                query_time(1)=ed.value.asInstanceOf[Long]
                query_sets=get_Partition(query_time(0),query_time(1),bounds)
            })

            val pruned=new PartitionPruningRDD[IndexedPartition](steid.temporalRDD.temporalPartition,query_sets.contains)

            val res=pruned.flatMap(packed => {

              val data = packed.data
              val index=packed.index.asInstanceOf[STEIDIndex[Int]]
              val min_time=index.mintime
              val max_time=index.maxtime
              var mid_partition_flag: Boolean =false
              val basetime = index.basetime
              var result:ArrayBuffer[Int]=new ArrayBuffer[Int]()

              if(single_attr){
                if(query_time(1)-basetime<min_time || query_time(0)-basetime>max_time){
                  None
                }else{
                  val st_index=index.ST
                  var res:Array[Int]=Array()
                  for(i<- (query_time(0)-basetime).toInt to (query_time(1)-basetime).toInt){
                    if(st_index.containsKey(i)){
                      res ++= st_index.get(i)
                    }
                  }

                  val query_result=res.map(iter=>data(iter))
                  if(flag_agge==1){
                    //max aggerator
                    Array(query_result.map(iter=>(iter.getLong(order),iter)).maxBy(_._1)._2).iterator
                  }else if(flag_agge==2){
                    //min aggerator
                    Array(query_result.map(iter=>(iter.getLong(order),iter)).minBy(_._1)._2).iterator
                  }else if(flag_agge==3){
                    //mean aggerator
                    val All_Value = res.map(iter => data(iter).getLong(aggerator_order.toInt))
                    val Sum_value = All_Value.sum
                    val Mean_value = Sum_value / res.length
                    query_result.filter(iter=>iter.getLong(order) >= Mean_value)
                  }else{
                    //range or temporal query
                    query_result
                  }
                }
              }
              else{
                //interval_query
                if(query_time(0)>=min_time && query_time(1)<=max_time) {
                  result = packed.index.asInstanceOf[STEIDIndex[Int]].getResult(query_time(0), query_time(1))
                }else if(query_time(0)<min_time && query_time(1)>max_time){

                  mid_partition_flag=true
                }else if(query_time(0)<max_time &&query_time(0)>=min_time &&query_time(1)>max_time){
                  result=packed.index.asInstanceOf[STEIDIndex[Int]].getResult(query_time(0), max_time)
                }else{
                  result=packed.index.asInstanceOf[STEIDIndex[Int]].getResult(min_time, query_time(1))
                }
              }

              if(query_time(0)<min_time && query_time(1)<min_time){
                None
              }
              else if(mid_partition_flag){
                data
              }else {
                result.distinct.map(iter => packed.data(iter))
              }
            })

            if(res.count()==1){
              res
            }else if(flag_agge == 1){
              val value = res.reduce((a, b) => if (a.getLong(order) > b.getLong(order)) a else b).getLong(order)
              res.filter(_.getLong(order) == value)
            }else if(flag_agge == 2){
              val value = res.reduce((a, b) => if (a.getLong(order) < b.getLong(order)) a else b).getLong(order)
              res.filter(_.getLong(order) == value)
            }else if(flag_agge == 3){
              val All_Value = res.map(iter => iter.getLong(0))
              val Sum_value = All_Value.sum
              val Mean_value = Sum_value / res.count()
              res.filter(iter => iter.getLong(order) >= Mean_value)
            }else{
              res
            }

          }).reduce((a,b)=>a.union(b))
        }
        else
          steid.temporalRDD.temporalPartition.flatMap(_.data)

      case _ =>
        null
    }

    after_filter
  }

  override def output: Seq[Attribute] = attributes
}

