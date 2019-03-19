package org.apache.spark.sql.timo.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

case class InRange(attr:Expression,start:Expression,end:Expression)
  extends Predicate with CodegenFallback {

  override def children: Seq[Expression] = Seq(attr,start,end)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = true

  override def toString(): String = s" **Range ($attr) In ($start-$end)"

}