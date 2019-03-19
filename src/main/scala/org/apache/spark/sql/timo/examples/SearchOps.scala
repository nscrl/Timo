package org.apache.spark.sql.timo.examples

import org.apache.spark.sql.timo.TimoSession
import org.apache.spark.sql.timo.index.{HashType, STEIDType}

/**
  * Created by Elroy on 5/7/2017.
  */
object SearchOps extends App{

  case class Record(time:Long,attr1:Int,attr2:Int,attr3:Int,attr4:Int,attr5:Int)
  case class Name(name:String,event_time_st:Long,event_time_ed:Long,free:Int,location:Int)
  val timoSession = TimoSession
    .builder()
    .master("local[*]")
    .appName("SearchOps")
    .getOrCreate()

  timoSession.sessionState.setConf("spark.driver.maxResultSize","10g")
  timoSession.sessionState.setConf("timo.index.partitions",10.toString)

  Time_Range(timoSession)
  timoSession.stop()

  private def Time_Range(session: TimoSession): Unit ={

    import session.implicits._
    import session.TimoImplicits._
    val data=session.sparkContext.textFile(".../Timo/data_source.txt").map(_.toString.trim.split(",")).filter(_.length>=3).map(p=>{
      Record(p(0).toLong,p(1).toInt,p(2).toInt,p(3).toInt,p(4).toInt,p(5).toInt)
    }).toDS()

    data.index(HashType,"hash",Array("time"),"Month")
    data.Range_Find("time",20180104041617L,20180104072614L).show()
  }

}