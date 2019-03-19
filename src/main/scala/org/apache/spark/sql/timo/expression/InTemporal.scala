package org.apache.spark.sql.timo.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}

/**
  * Created by houkailiu on 2017/7/8.
  */
case class InTemporal(first:Expression,secound:Expression)
  extends Predicate with CodegenFallback {

  override def children: Seq[Expression] =Seq(first,secound)

  override def nullable: Boolean = false

  override def toString: String = s" **($first) OverLap Time($secound)"

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any={
    //false
    if(input.getLong(0)==Literal(1).toString().toLong)
      true
    else {
      false
    }
  }

  def intersects(other:Long,min:Long,max:Long): Boolean ={
    if(other>max||other<min)
      false
    else
      true
  }
}
