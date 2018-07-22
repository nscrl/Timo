package org.apache.spark.sql.timo

import java.beans.Introspector
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{Encoder, Row, SQLContext, SparkSession, DataFrame => SQLDataFrame, Dataset => SQLDataset}
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.timo.index.IndexType
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * Created by houkailiu on 5/11/17.
  */
class TimoSession private[timo](@transient override val sparkContext: SparkContext)
  extends SparkSession(sparkContext) { self =>

  @transient
  private[sql] override lazy val sessionState: TimoSessionState = {
    new TimoSessionState(this)
  }

  def hasIndex(tableName: String, indexName: String): Boolean = {
    sessionState.indexManager.lookupIndexedData(table(tableName), indexName).nonEmpty
  }

  def indexTable(tableName: String, indexType: IndexType,
                 indexName: String, column: Array[String]): Unit = {
    val tbl = table(tableName)
    assert(tbl != null, "Table not found")
    val attrs = tbl.queryExecution.analyzed.output
    val columnKeys = column.map(attr => {
      var ans: Attribute = null
      for (i <- attrs.indices)
        if (attrs(i).name.equals(attr)) ans = attrs(i)
      assert(ans != null, "Attribute not found")
      ans
    }).toList
    sessionState.indexManager.createIndexQuery(table(tableName), indexType,
      indexName, columnKeys, Some(tableName))
  }

  def indexTable2(tableName: String, indexType: IndexType,
                 indexName: String, column:Array[String]): Unit = {
    val tbl = table(tableName)
    assert(tbl != null, "Table not found")

    val attrs = tbl.queryExecution.analyzed.output
    val columnKeys = column.map(attr => {
      var ans: Attribute = null
      for (i <- attrs.indices)
        if (attrs(i).name.equals(attr)) ans = attrs(i)
      assert(ans != null, "Attribute not found")
      ans
    }).toList
    println(columnKeys)
    sessionState.indexManager.createIndexQuery(table(tableName), indexType, indexName, columnKeys, Some(tableName))
  }

  def showIndex(tableName: String): Unit = sessionState.indexManager.showQuery(tableName)

  object TimoImplicits extends Serializable {
    protected[timo] def _timoContext: SparkSession = self

    implicit def datasetTotimoDataSet[T : Encoder](ds: SQLDataset[T]): Dataset[T] =
      Dataset(self, ds.queryExecution.logical)

    implicit def dataframeTotimoDataFrame(df: SQLDataFrame): DataFrame =
      Dataset.ofRows(self, df.queryExecution.logical)
  }
}

object TimoSession {


  class Builder extends Logging {

    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
      userSuppliedContext = Option(sparkContext)
      this
    }



    def appName(name: String): Builder = config("spark.app.name", name)

    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    def config(conf: SparkConf): Builder = synchronized {
      conf.getAll.foreach { case (k, v) => options += k -> v }
      this
    }

    def master(master: String): Builder = config("spark.master", master)

    def enableHiveSupport(): Builder = synchronized {
      if (hiveClassesArePresent) {
        config(CATALOG_IMPLEMENTATION.key, "hive")
      } else {
        throw new IllegalArgumentException(
          "Unable to instantiate sasSession with Hive support because " +
            "Hive classes are not found.")
      }
    }

    def getOrCreate(): TimoSession = synchronized {
      // Get the session from current thread's active session.
      var session = activeThreadSession.get()
      if ((session ne null) && !session.sparkContext.isStopped) {
        options.foreach { case (k, v) => session.sessionState.setConf(k, v) }
        if (options.nonEmpty) {
          logWarning("Using an existing sasSession; some configuration may not take effect.")
        }
        return session
      }

      // Global synchronization so we will only set the default session once.
      TimoSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          options.foreach { case (k, v) => session.sessionState.setConf(k, v) }
          if (options.nonEmpty) {
            logWarning("Using an existing sasSession; some configuration may not take effect.")
          }
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          // set app name if not given
          val randomAppName = java.util.UUID.randomUUID().toString
          val sparkConf = new SparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(randomAppName)
          }
          val sc = SparkContext.getOrCreate(sparkConf)
          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by sasSession
          options.foreach { case (k, v) => sc.conf.set(k, v) }
          if (!sc.conf.contains("spark.app.name")) {
            sc.conf.setAppName(randomAppName)
          }
          sc
        }
        session = new TimoSession(sparkContext)
        options.foreach { case (k, v) => session.sessionState.setConf(k, v) }
        defaultSession.set(session)

        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            defaultSession.set(null)
            sqlListener.set(null)
          }
        })
      }

      return session
    }
  }

  def builder(): Builder = new Builder

  def setActiveSession(session: TimoSession): Unit = {
    activeThreadSession.set(session)
  }

  def clearActiveSession(): Unit = {
    activeThreadSession.remove()
  }

  def setDefaultSession(session: TimoSession): Unit = {
    defaultSession.set(session)
  }

  def clearDefaultSession(): Unit = {
    defaultSession.set(null)
  }

  private[sql] def createDataFrame(rdd: RDD[_], beanClass: Class[_],sASSession: TimoSession): Dataset[Row] = {
    val attributeSeq: Seq[AttributeReference] = getSchema(beanClass)
    val className = beanClass.getName
    val rowRdd = rdd.mapPartitions { iter =>
      // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      val localBeanInfo = Introspector.getBeanInfo(Utils.classForName(className))
      SQLContext.beansToRows(iter, localBeanInfo, attributeSeq)
    }
    Dataset.ofRows(sASSession, LogicalRDD(attributeSeq, rowRdd)(sASSession))
  }

  private[sql] def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    val (dataType, _) = JavaTypeInference.inferDataType(beanClass)
    dataType.asInstanceOf[StructType].fields.map { f =>
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
  }

  private[sql] def createDataFrame1(rdd: JavaRDD[_], beanClass: Class[_],sASSession: TimoSession):Dataset[Row] = {
    createDataFrame(rdd.rdd, beanClass,sASSession)
  }


  private[sql] def getActiveSession: Option[TimoSession] = Option(activeThreadSession.get)

  private[sql] def getDefaultSession: Option[TimoSession] = Option(defaultSession.get)

  private[sql] val sqlListener = new AtomicReference[SQLListener]()

  private val activeThreadSession = new InheritableThreadLocal[TimoSession]

  private val defaultSession = new AtomicReference[TimoSession]

  private val HIVE_SESSION_STATE_CLASS_NAME = "org.apache.spark.sql.hive.HiveSessionState"

  private def sessionStateClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => HIVE_SESSION_STATE_CLASS_NAME
      case "in-memory" => classOf[SessionState].getCanonicalName
    }
  }

  private def reflect[T, Arg <: AnyRef](
                                         className: String,
                                         ctorArg: Arg)(implicit ctorArgTag: ClassTag[Arg]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag.runtimeClass)
      ctor.newInstance(ctorArg).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  private[spark] def hiveClassesArePresent: Boolean = {
    try {
      Utils.classForName(HIVE_SESSION_STATE_CLASS_NAME)
      Utils.classForName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }
}