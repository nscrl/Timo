package org.apache.spark.sql.timo.util


import org.apache.spark.sql.catalyst.expressions.Literal

/**
  * Created by houkailiu on 5/28/2016.
  */
object LiteralUtil {
  def apply(v: Any): Literal = v match {
    case _ => Literal(v)
  }
}
