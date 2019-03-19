package org.apache.spark.sql.timo.util

import org.apache.spark.sql.catalyst.InternalRow


object MaxOrdering extends Ordering[InternalRow] with Serializable{
  def compare(a:InternalRow,b:InternalRow): Int =a.getLong(1) compare b.getLong(1)
}

object MinOrdering extends Ordering[InternalRow] with Serializable{
  override def compare(x: InternalRow, y: InternalRow): Int = {
    x.getLong(1) compare y.getLong(1)
  }
}