package org.apache.spark.sql.timo.index


trait Index
object IndexType {
  def apply(indextype: String): IndexType=indextype.toLowerCase match{

    case "HashType"=>HashType

    case "IntervalTree" =>IntervalType

    case "STEIDType"=>STEIDType

    case _=>null
  }
}

sealed abstract class IndexType
case object IntervalType extends IndexType
case object HashType extends IndexType
case object STEIDType extends IndexType