package org.apache.spark.sql.timo

import java.util.Properties

import scala.collection.{immutable, mutable}
import scala.collection.JavaConverters._
/**
  * Created by Elroy on 5/11/16.
  * Configuration
  */
private[timo] object TimoConf {
  private val TimoConfEntries = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, TimoConfEntry[_]]())

  private[timo] class TimoConfEntry[T] private(
      val key: String,
      val defaultValue: Option[T],
      val valueConverter: String => T,
      val stringConverter: T => String,
      val doc: String,
      val isPublic: Boolean) {
    def defaultValueString: String = defaultValue.map(stringConverter).getOrElse("<undefined>")

    override def toString: String = {
      s"TimoConfEntry(key = $key, defaultValue=$defaultValueString, doc=$doc, isPublic = $isPublic)"
    }
  }

  private[timo] object TimoConfEntry {
    private def apply[T](key: String, defaultValue: Option[T], valueConverter: String => T,
                         stringConverter: T => String, doc: String, isPublic: Boolean): TimoConfEntry[T] =
      TimoConfEntries.synchronized {
        if (TimoConfEntries.containsKey(key)) {
          throw new IllegalArgumentException(s"Duplicate TimoConfEntry. $key has been registered")
        }
        val entry =
          new TimoConfEntry[T](key, defaultValue, valueConverter, stringConverter, doc, isPublic)
        TimoConfEntries.put(key, entry)
        entry
      }

    def intConf(key: String, defaultValue: Option[Int] = None,
                doc: String = "", isPublic: Boolean = true): TimoConfEntry[Int] =
      TimoConfEntry(key, defaultValue, { v =>
        try {
          v.toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be int, but was $v")
        }
      }, _.toString, doc, isPublic)

    def longConf(key: String, defaultValue: Option[Long] = None,
                 doc: String = "", isPublic: Boolean = true): TimoConfEntry[Long] =
      TimoConfEntry(key, defaultValue, { v =>
        try {
          v.toLong
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be long, but was $v")
        }
      }, _.toString, doc, isPublic)

    def doubleConf(key: String, defaultValue: Option[Double] = None,
                   doc: String = "", isPublic: Boolean = true): TimoConfEntry[Double] =
      TimoConfEntry(key, defaultValue, { v =>
        try {
          v.toDouble
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be double, but was $v")
        }
      }, _.toString, doc, isPublic)

    def booleanConf(key: String, defaultValue: Option[Boolean] = None,
                    doc: String = "", isPublic: Boolean = true): TimoConfEntry[Boolean] =
      TimoConfEntry(key, defaultValue, { v =>
        try {
          v.toBoolean
        } catch {
          case _: IllegalArgumentException =>
            throw new IllegalArgumentException(s"$key should be boolean, but was $v")
        }
      }, _.toString, doc, isPublic)

    def stringConf(key: String, defaultValue: Option[String] = None,
                   doc: String = "", isPublic: Boolean = true): TimoConfEntry[String] =
      TimoConfEntry(key, defaultValue, v => v, v => v, doc, isPublic)
  }

  import TimoConfEntry._

  val Topk=stringConf("timo.topk",defaultValue = Some("Slot"))

  val INDEX_PARTITIONS = intConf("timo.index.partitions", defaultValue = Some(4))

  // Partitioner Parameter
  val PARTITION_METHOD = stringConf("timo.partition.method", defaultValue = Some("STRPartitioner"))

  val PERIODTYPE=stringConf("timo.partition.period",defaultValue = Some("0"))

  var AGGERATOR_ORDER=stringConf("timo.aggerator.order",defaultValue = Some("1"))

  val AGGERATOR_ATTR=stringConf("timo.aggerator.attr",defaultValue = Some("0"))

  val RANGE_SEARCH=stringConf("timo.aggerator.range",defaultValue = Some("0"))

  val INTERVAL_SEARCH=stringConf("timo.aggerator.interval",defaultValue = Some("0"))

  val INDEX_ATTR_NUM=intConf("timo.index.attrnum",defaultValue = Some(1))

  val WHICH_ATTR=intConf("timo.index.whichattr",defaultValue = Some(0))

  val LEFT_ATTRIBUTE=stringConf("timo.join.leftattr",defaultValue = Some(""))

  val RIGHT_ATTRIBUTE=stringConf("timo.join.rightattr",defaultValue = Some(""))

  val HASH_MEMORY_TIME=intConf("timo.join.memorytime",defaultValue=Some(14))

  val GET_RESULT_TIME=intConf("timo.join.resulttime",defaultValue = Some(6))

  val EIIHBAE=stringConf("timo.eiihbase.operator",defaultValue = Some("false"))

}

private[timo] class TimoConf extends Serializable {
  import TimoConf._
  @transient protected[timo] val settings =
    new java.util.HashMap[String, String]()

  private[timo] def indexPartitions: Int = getConf(INDEX_PARTITIONS)

  private[timo] def topk:String=getConf(Topk)
  private[timo] def partitionMethod: String = getConf(PARTITION_METHOD)

  private[timo] def periodtype:String=getConf(PERIODTYPE)
  private[timo] def aggeratorOrder:String=getConf(AGGERATOR_ORDER)
  private[timo] def aggeratorAttr:String=getConf(AGGERATOR_ATTR)

  private[timo] def aggeratorRange:String=getConf(RANGE_SEARCH)
  private[timo] def indexAttrNum:Int=getConf(INDEX_ATTR_NUM)
  private[timo] def which_attr:mutable.HashMap[String,Int]=new mutable.HashMap[String,Int]()
  private[timo] def left_attrbute:String=getConf(LEFT_ATTRIBUTE)
  private[timo] def right_attrbute:String=getConf(RIGHT_ATTRIBUTE)

  private[timo] def memory_time:Int=getConf(HASH_MEMORY_TIME)
  private[timo] def result_time:Int=getConf(GET_RESULT_TIME)

  private[timo] def eiihbase:String=getConf(EIIHBAE)

  def setConf(props: Properties): Unit = settings.synchronized {
    props.asScala.foreach { case (k, v) => setConfString(k, v)
    }
  }

  def setConfString(key: String, value: String): Unit = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    val entry = TimoConfEntries.get(key)
    if (entry != null) {
      // Only verify configs in the SimbaConf object
      entry.valueConverter(value)//.valueConverter(value)
    }
    settings.put(key, value)
  }

  /*def setConfBoolean(key:String,value:Boolean):Unit={
    require(key!=null,"key cannot be null")
    require(value!=null ,s"value cannot be null for key:$key")
    val entry=TimoConfEntries.get(key)
    if(entry!=null){
      entry.valueConverter(value)
    }
    //settings.put(key,value)
  }*/

  def setConf[T](entry: TimoConfEntry[T], value: T): Unit = {
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    require(TimoConfEntries.get(entry.key) == entry, s"$entry is not registered")
    settings.put(entry.key, entry.stringConverter(value))
  }

  def getConfString(key: String): String = {
    Option(settings.get(key)).
      orElse {
        // Try to use the default value
        Option(TimoConfEntries.get(key)).map(_.defaultValueString)
      }.
      getOrElse(throw new NoSuchElementException(key))
  }

  def getConf[T](entry: TimoConfEntry[T], defaultValue: T): T = {
    require(TimoConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).getOrElse(defaultValue)
  }

  def getConf[T](entry: TimoConfEntry[T]): T = {
    require(TimoConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).orElse(entry.defaultValue).
      getOrElse(throw new NoSuchElementException(entry.key))
  }

  def getConfString(key: String, defaultValue: String): String = {
    val entry = TimoConfEntries.get(key)
    if (entry != null && defaultValue != "<undefined>") {
      // Only verify configs in the SimbaConf object
      entry.valueConverter(defaultValue)
    }
    Option(settings.get(key)).getOrElse(defaultValue)
  }

  def getAllConfs: immutable.Map[String, String] =
    settings.synchronized { settings.asScala.toMap }


  def getAllDefinedConfs: Seq[(String, String, String)] = TimoConfEntries.synchronized {
    TimoConfEntries.values.asScala.filter(_.isPublic).map { entry =>
      (entry.key, entry.defaultValueString, entry.doc)
    }.toSeq
  }

  private[timo] def unsetConf(key: String): Unit = {
    settings.remove(key)
  }

  private[timo] def unsetConf(entry: TimoConfEntry[_]): Unit = {
    settings.remove(entry.key)
  }

  private[timo] def clear(): Unit = {
    settings.clear()
  }
}
