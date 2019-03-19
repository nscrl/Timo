package org.apache.spark.sql.timo.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Predicate}

case class InMax(attr:Expression,start:Expression,end:Expression,flag:Expression)
  extends Predicate with CodegenFallback {

  override def children: Seq[Expression] = Seq(attr,start,end,flag)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = true

  override def toString(): String = s" **Max ($attr) In ($start-$end)"

}