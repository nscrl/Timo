package org.apache.spark.sql.timo.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}
import org.apache.spark.sql.timo.util.NumberUtil

/**
  * Created by mint on 17-5-15.
  */
case class InTime(point: Expression, timepoint: Expression,timepoint1:Expression)
  extends Predicate with CodegenFallback {

  override def children: Seq[Expression] =Seq(point,timepoint,timepoint1)

  override def nullable: Boolean = false

  override def toString: String = s" **($point) IN Time($timepoint-$timepoint1)"

  // XX Tricky hack
  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any={
    val eval_shape=input.getString(3).toInt
    val test1=input.getLong(0)
    val test2=input.getLong(1)
    val test3=input.getLong(2)
    val eval_low=timepoint.asInstanceOf[Literal].value.asInstanceOf[Long]
    val eval_high=timepoint1.asInstanceOf[Literal].value.asInstanceOf[Long]
    if(eval_shape>eval_high||eval_shape<eval_low)
      false
    else
      true
    //intersects(eval_shape,eval_low,eval_high)
//    val mbr=MBR1
  }

  def intersects(other:Long,min:Long,max:Long): Boolean ={
    if(other>max||other<min)
      false
    else
      true
  }
}
