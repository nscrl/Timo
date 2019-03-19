package org.apache.spark.sql.timo.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

/**
  * Created by mint on 17-6-6.
  */
case class InTopK(point: Expression, timepoint: Expression,timepoint1:Expression,k:Expression,flag:Expression)
  extends Predicate with CodegenFallback {

  override def children: Seq[Expression] =Seq(point,timepoint,timepoint1,k)

  override def nullable: Boolean = false

  //def hasTemporal=true

  override def toString: String = s" **($point) IN Time($timepoint-$timepoint1)"

  // XX Tricky hack
  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any={
    val eval_shape=point.eval(input).asInstanceOf[Long]
    val eval_low=timepoint.asInstanceOf[Literal].value.asInstanceOf[Long]
    val eval_high=timepoint1.asInstanceOf[Literal].value.asInstanceOf[Long]
    if(eval_shape>eval_high||eval_shape<eval_low)
      false
    else
      true
  }

  def intersects(other:Long,min:Long,max:Long): Boolean ={
    if(other>max||other<min)
      false
    else
      true
  }
}
