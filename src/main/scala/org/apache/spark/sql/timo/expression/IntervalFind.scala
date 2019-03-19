package org.apache.spark.sql.timo.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Predicate}

/**
  * Created by houkailiu on 2017/7/17.
  */
case class IntervalFind(attr :Expression,start_point:Expression,end_point:Expression,flag:Expression)
  extends Predicate with CodegenFallback{

  override def children: Seq[Expression] =Seq(attr,start_point,end_point,flag)

  override def nullable: Boolean = false

  override def toString: String = s" **($attr) IN Time($start_point-$end_point)"

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any={
      true
  }

}