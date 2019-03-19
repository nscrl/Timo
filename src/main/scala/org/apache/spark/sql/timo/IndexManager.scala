package org.apache.spark.sql.timo

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.timo.index._
import org.apache.spark.sql.{Dataset => SQLDataset}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by houkailiu on 5/4/17.
  * Index Manager for Timo
  */
private case class IndexedData(name: String, plan: LogicalPlan, indexedData: IndexedRelation)

case class IndexInfo(tableName: String, indexName: String,
                     attributes: Seq[Attribute], indexType: IndexType,
                     location: String, storageLevel: StorageLevel) extends Serializable

private[timo] class IndexManager extends Logging with Serializable {
  @transient
  private val indexedData = new ArrayBuffer[IndexedData]

  @transient
  private val indexLock = new ReentrantReadWriteLock

  @transient
  private val indexInfos = new ArrayBuffer[IndexInfo]

  def getIndexInfo: Array[IndexInfo] = indexInfos.toArray

  private def readLock[A](f: => A): A = {
    val lock = indexLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  private def writeLock[A](f: => A): A = {
    val lock = indexLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  private[timo] def isEmpty: Boolean = readLock {
    indexedData.isEmpty
  }

  private[timo] def lookupIndexedData(query: SQLDataset[_]): Option[IndexedData] = readLock {
    val tmp_res = indexedData.find(cd => query.queryExecution.analyzed.sameResult(cd.plan))
    if (tmp_res.nonEmpty) return tmp_res
    else {
      indexedData.find(cd => {
        cd.plan match {
          case tmp_plan: SubqueryAlias =>
            query.queryExecution.analyzed.sameResult(tmp_plan.child)
          case _ => false
        }
      })
    }
  }

  private[timo] def lookupIndexedData(plan: LogicalPlan): Option[IndexedData] = readLock {
    val tmp_res = indexedData.find(cd => plan.sameResult(cd.plan))
    if (tmp_res.nonEmpty) return tmp_res
    else {
      indexedData.find(cd => {
        cd.plan match {
          case tmp_plan: SubqueryAlias =>
            plan.sameResult(tmp_plan.child)
          case _ => false
        }
      })
    }
  }

  private[timo] def lookupIndexedData(query: SQLDataset[_], indexName: String): Option[IndexedData] =
    readLock {
      lookupIndexedData(query.queryExecution.analyzed, indexName)
    }

  private[timo] def lookupIndexedData(plan: LogicalPlan, indexName: String): Option[IndexedData] =
    readLock {
      val tmp_res = indexedData.find(cd => plan.sameResult(cd.plan) && cd.name.equals(indexName))
      if (tmp_res.nonEmpty) return tmp_res
      else {
        indexedData.find(cd => {
          cd.plan match {
            case tmp_plan: SubqueryAlias =>
              plan.sameResult(tmp_plan.child) && cd.name.equals(indexName)
            case _ => false
          }
        })
      }
    }

  private[timo] def setStorageLevel(query: SQLDataset[_], indexName: String, newLevel: StorageLevel): Unit = writeLock {
    val dataIndex = indexedData.indexWhere {
      cd => query.queryExecution.analyzed.sameResult(cd.plan) && cd.name.equals(indexName)
    }
    require(dataIndex >= 0, "Index not found!")
    val preData = indexInfos(dataIndex)
    indexInfos(dataIndex) = IndexInfo(preData.tableName, preData.indexName, preData.attributes,
      preData.indexType, preData.location, newLevel)
  }

  private[timo] def createIndexQuery(query: SQLDataset[_], indexType: IndexType, indexName: String,
                                    column: List[Attribute], tableName: Option[String] = None,
                                    storageLevel: StorageLevel = MEMORY_AND_DISK): Unit =
    writeLock {
      val planToIndex = query.queryExecution.analyzed
      if (lookupIndexedData(planToIndex).nonEmpty) {
        // scalastyle:off println
        println("Index for the data has already been built.")
        // scalastyle:on println
      } else {
        indexedData +=
          IndexedData(indexName, planToIndex,
            IndexedRelation(query.queryExecution.executedPlan, tableName, indexType, column, indexName))
        indexInfos += IndexInfo(tableName.getOrElse("NoName"), indexName, column, indexType, "", storageLevel)
      }
    }

  private[timo] def showQuery(tableName: String): Unit = readLock {
    indexInfos.map(row => {
      if (row.tableName.equals(tableName)) {
        // scalastyle:off println
        println("Index " + row.indexName + " {")
        println("\tTable: " + tableName)
        print("\tOn column: (")
        for (i <- row.attributes.indices)
          if (i != row.attributes.length - 1) {
            print(row.attributes(i).name + ", ")
          } else println(row.attributes(i).name + ")")
        println("\tIndex Type: " + row.indexType.toString)
        println("}")
        // scalastyle:on println
      }
      row
    })
  }

  private[timo] def dropIndexQuery(query: Dataset[_], blocking: Boolean = true): Unit = writeLock {
    val planToIndex = query.queryExecution.analyzed
    var hasFound = false
    var found = true
    while (found) {
      val dataIndex = indexedData.indexWhere(cd => planToIndex.sameResult(cd.plan))
      if (dataIndex < 0) found = false
      else hasFound = true
      indexedData(dataIndex).indexedData.temporalRDD.temporalPartition.unpersist(blocking)
      indexedData.remove(dataIndex)
      indexInfos.remove(dataIndex)
    }
    indexedData
  }

  private[timo] def dropIndexByNameQuery(query: SQLDataset[_],
                                        indexName: String,
                                        blocking: Boolean = true): Unit = writeLock {
    val planToIndex = query.queryExecution.analyzed
    val dataIndex = indexedData.indexWhere { cd =>
      planToIndex.sameResult(cd.plan) && cd.name.equals(indexName)
    }
    require(dataIndex >= 0, s"Table $query or index $indexName is not indexed.")
    indexedData(dataIndex).indexedData.temporalRDD.temporalPartition.unpersist(blocking)
    indexedData.remove(dataIndex)
    indexInfos.remove(dataIndex)
  }

  private[timo] def dropIndexByColumnQuery(query: SQLDataset[_],
                                          column: List[Attribute],
                                          blocking: Boolean = true): Unit = writeLock {
    val planToIndex = query.queryExecution.analyzed
    var dataIndex = -1
    for (i <- 0 to indexInfos.length) {
      val cd = indexedData(i)
      val row = indexInfos(i)
      if (planToIndex.sameResult(cd.plan) && row.attributes.equals(column)) {
        dataIndex = i
      }
    }
    require(dataIndex >= 0, s"Table $query or Index on $column is not indexed.")
    indexedData(dataIndex).indexedData.temporalRDD.temporalPartition.unpersist(blocking)
    indexedData.remove(dataIndex)
    indexInfos.remove(dataIndex)
  }

  private[timo] def tryDropIndexQuery(query: SQLDataset[_],
                                     blocking: Boolean = true): Boolean = writeLock {
    val planToIndex = query.queryExecution.analyzed
    var found = true
    var hasFound = false
    while (found) {
      val dataIndex = indexedData.indexWhere(cd => planToIndex.sameResult(cd.plan))
      found = dataIndex >= 0
      if (found) {
        hasFound = true
        indexedData(dataIndex).indexedData.temporalRDD.temporalPartition.unpersist(blocking)
        indexedData.remove(dataIndex)
        indexInfos.remove(dataIndex)
      }
    }
    hasFound
  }

  private[timo] def tryDropIndexByNameQuery(query: SQLDataset[_],
                                           indexName: String,
                                           blocking: Boolean = true): Boolean = writeLock {
    val planToCache = query.queryExecution.analyzed
    val dataIndex = indexedData.indexWhere(cd => planToCache.sameResult(cd.plan))
    val found = dataIndex >= 0
    if (found) {
      indexedData(dataIndex).indexedData.temporalRDD.temporalPartition.unpersist(blocking)
      indexedData.remove(dataIndex)
      indexInfos.remove(dataIndex)
    }
    found
  }

  private[timo] def clearIndex(): Unit = writeLock {
    indexedData.foreach(_.indexedData.temporalRDD.temporalPartition.unpersist())
    indexedData.clear()
    indexInfos.clear()
  }

  private[timo] def useIndexedData(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {
      case currentFragment =>
        lookupIndexedData(currentFragment)
          .map(_.indexedData.withOutput(currentFragment.output))
          .getOrElse(currentFragment)
    }
  }

  private[timo] def persistIndex(timoSession: TimoSession,
                                indexName: String,
                                fileName: String): Unit = {
    val dataIndex = indexedData.indexWhere(cd => cd.name.equals(indexName))
    require(dataIndex >= 0, "Index not found!")
    val preData = indexInfos(dataIndex)
    val indexedItem = indexedData(dataIndex)
    val sparkContext = timoSession.sparkContext

    if (preData.indexType == STEIDType) {

      val steidRelation = indexedItem.indexedData.asInstanceOf[STEIDIndexRelation]
      sparkContext.parallelize(steidRelation.temporalRDD.bounds).saveAsObjectFile("hdfs://master:9000/index/"+fileName+"/bounds")
      sparkContext.parallelize(Array(steidRelation)).saveAsObjectFile("hdfs://master:9000/index/"+fileName + "/steidRelation")
    } else{
      val hashRelation = indexedItem.indexedData.asInstanceOf[HashIndexRelation]
      sparkContext.parallelize(hashRelation.temporalRDD.bounds).saveAsObjectFile("hdfs://master:9000/index/"+fileName+"/bounds")
      sparkContext.parallelize(Array(hashRelation)).saveAsObjectFile("hdfs://master:9000/index/"+fileName + "/steidRelation")
      hashRelation.temporalRDD.temporalPartition.saveAsObjectFile("hdfs://master:9000/index/"+fileName + "/rdd")
    }

    indexInfos(dataIndex) = IndexInfo(preData.tableName, preData.indexName,
      preData.attributes, preData.indexType, fileName, preData.storageLevel)
  }

  private[timo] def loadIndex(timoSession: TimoSession, indexName: String, fileName: String): Unit = {

  }
}