package org.apache.spark.sql.timo.plans

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, UnaryNode}
import org.apache.spark.sql.timo.execution.TopK.SlotTopk
import org.apache.spark.sql.types.BooleanType


case class TemporalTopk(logical: LogicalPlan,attrs:Seq[Attribute],
                        startpoint:Literal,endpoint:Literal,k:Literal)
  extends UnaryNode with PredicateHelper  {

  override def child: LogicalPlan = logical

  override def output: Seq[Attribute] = child.output

  override def maxRows: Option[Long] = child.maxRows

}