package org.apache.spark.sql.timo.examples

import org.apache.spark.sql.timo.TimoSession
import org.apache.spark.sql.timo.examples.SearchOps.Record
import org.apache.spark.sql.timo.index.HashType
/**
  * Created by Elroy on 5/7/2017.
  */

object AnalyseOps extends App {

  val timoSession: TimoSession = TimoSession
    .builder()
    .master("local[*]")
    .appName("AnalyseOps")
    .getOrCreate()

  timoSession.sessionState.setConf("spark.serializer","org.apache.serializer.KryoSerializer")
  timoSession.sessionState.setConf("spark.kryo.registrationRequired", "true")
  timoSession.sessionState.setConf("timo.index.partitions",10.toString)

  Get_Min(timoSession)

  timoSession.stop()

  private def Get_Min(timosession: TimoSession)={
    import timosession.implicits._
    import timosession.TimoImplicits._
    val data=timosession.sparkContext.textFile(".../Timo/data_source.txt").map(_.toString.trim.split(",")).filter(_.length>=3).map(p=>{
      Record(p(0).toLong,p(1).toInt,p(2).toInt,p(3).toInt,p(4).toInt,p(5).toInt)
    }).toDS()

    data.index(HashType,"hash",Array("time"),"Month")
    data.Min("attr3",20180104041617L,20180104072614L)
  }
}
