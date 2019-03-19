package org.apache.spark.sql.timo.partitioner

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.{CollectionsUtils, MutablePair}
import org.apache.spark.{Partitioner, SparkEnv}

import scala.collection.mutable.ArrayBuffer

object TemporalPartitioner {

  def sortBasedShuffleOn:Boolean=SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  def apply(origin:RDD[(Long,InternalRow)],period:Long,partition_num:Int):(RDD[InternalRow],Array[Long],Long)={

    val rdd=if(sortBasedShuffleOn){
      origin.mapPartitions{iter=>iter.map(row=>(row._1,row._2.copy()))}
    }else{
      origin.mapPartitions{iter=>
        val mutablePair=new MutablePair[Long,InternalRow]()
        iter.map(row=>mutablePair.update(row._1,row._2.copy()))
      }
    }
    //2017 01 01 01 01 01
    val sort_data=rdd.sortBy(iter=> iter._1)
    val mintime=rdd.map(iter=>iter._2.getLong(1)).min()
    val dataset=sort_data.map(iter=>iter._1).collect().sorted//.sortBy(_._1).map()

    val part=new TemporalPartitioner(dataset,ascending = true,period,partition_num)
    val shuffled=new ShuffledRDD[Long,InternalRow,InternalRow](sort_data,part)
    (shuffled.map(iter=>iter._2),part.partition_bounds,mintime)
  }


}

class TemporalPartitioner[T](dataset:Array[Long],
                             private var ascending: Boolean = true,
                             period:Long,
                             partitions:Int) extends Partitioner{

  override def numPartitions: Int = partition_bounds.length

  var determine: Array[Long] =determineBounds(dataset)

  var partition_bounds:Array[Long]=determine

  def getPartition(key: Any): Int = {
    val k=key.asInstanceOf[Long]
    partition_bounds.indexWhere(elem => elem >= k)
  }

  def determineBounds(data:Array[Long]):Array[Long]={

    val all_count=data.length
    val partition_record_num=all_count/partitions

    var bounds:ArrayBuffer[Long]=new ArrayBuffer[Long]()

    val Period_Data = data.map(iter => iter / period)
    //data 2017 01 01 01 01 01
    //确定数据集中的年数,月数和天数
    val p_Array=Period_Data.distinct

    var count_current_num=0L

    for(i<-p_Array.indices) {

      val period_data = Period_Data.filter(iter => iter == p_Array(i))
      val count = period_data.length
      val partition_num = (count * partitions) / all_count

      if (count < partition_record_num) {

        //var min_time = data(count_current_num.toInt)._2
        val max_time = data(count_current_num.toInt + count.toInt - 1)

        //result ++= ArrayBuffer(data(count_current_num.toInt + count.toInt - 1))
        bounds ++= ArrayBuffer(max_time)
        count_current_num += count
      } else {
        val interval = count / (partition_num + 1)

        for(i<-1 to partition_num){
          bounds ++= ArrayBuffer(data(count_current_num.toInt+i*interval))
        }
        bounds ++= ArrayBuffer(data(count_current_num.toInt+count))

        count_current_num += count

        /*while (auto_scan < partition_num * interval) {
          val compared = data(auto_scan + count_current_num.toInt)
          var move_scan = auto_scan + count_current_num.toInt

          while (data(move_scan) < compared) {

            val stop_time = data(move_scan)

            if (maxtime < stop_time)
              maxtime = stop_time

            current_num += 1

            if ((current_num - interval == 0)) {

              result += data(move_scan)
              current_num = 0
              bounds ++= ArrayBuffer(maxtime)
            }

            move_scan += 1
          }

          if ((current_num - interval == 0)) {
            result += data(move_scan)
            current_num = 0
            bounds ++= ArrayBuffer(maxtime)
            move_scan+=1
          }

          if (data(move_scan) >= compared) {
            move_scan += 1
            current_num += 1
          }

          auto_scan = move_scan-count_current_num.toInt
        }

        var max_time=0L
        for(i<- auto_scan to count){
          if(data(count_current_num.toInt+auto_scan)>max_time){
            max_time=data(count_current_num.toInt+auto_scan)
          }
        }
        bounds ++=ArrayBuffer(max_time)
        result++=ArrayBuffer(data(count_current_num.toInt+count-1))

        count_current_num += count*/
      }
    }

    bounds.toArray
  }

  private var binarySearch: (Array[Long], Long) => Int = CollectionsUtils.makeBinarySearch[Long]

  override def equals(other: Any): Boolean = other match {
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < partition_bounds.length) {
      result = prime * result + partition_bounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

}