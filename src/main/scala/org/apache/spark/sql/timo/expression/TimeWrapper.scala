package org.apache.spark.sql.timo.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, LongType}

/**
  * Created by mint on 17-5-22.
  */
case class TimeWrapper (exps: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override def children: Seq[Expression] = exps

  override def eval(input: InternalRow): Long = {
    1L
  }
}
