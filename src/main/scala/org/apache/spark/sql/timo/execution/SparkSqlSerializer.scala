package org.apache.spark.sql.timo.execution

import java.nio.ByteBuffer
import java.util.{HashMap => JavaHashMap}

import com.clearspring.analytics.stream.cardinality.HyperLogLog
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.ResourcePool
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer.{KryoSerializer, SerializerInstance}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.Decimal
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.OpenHashSet

import scala.reflect.ClassTag

private[sql] class SparkSqlSerializer(conf: SparkConf) extends KryoSerializer(conf) {
  override def newKryo(): Kryo = {
    val kryo = super.newKryo()
    kryo.setRegistrationRequired(false)
    kryo.register(classOf[MutablePair[_, _]])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericRow])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow])
//    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericMutableRow])
    kryo.register(classOf[com.clearspring.analytics.stream.cardinality.HyperLogLog],
                  new HyperLogLogSerializer)
    kryo.register(classOf[java.math.BigDecimal], new JavaBigDecimalSerializer)
    kryo.register(classOf[BigDecimal], new ScalaBigDecimalSerializer)

    // Specific hashsets must come first TODO: Move to core.
//    kryo.register(classOf[IntegerHashSet], new IntegerHashSetSerializer)
//    kryo.register(classOf[LongHashSet], new LongHashSetSerializer)
    kryo.register(classOf[org.apache.spark.util.collection.OpenHashSet[_]],
                  new OpenHashSetSerializer)
    kryo.register(classOf[Decimal])
    kryo.register(classOf[JavaHashMap[_, _]])

    kryo.setReferences(false)
    kryo
  }
}

private[execution] class KryoResourcePool(size: Int)
    extends ResourcePool[SerializerInstance](size) {

  val ser: SparkSqlSerializer = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new SparkSqlSerializer(sparkConf)
  }

  def newInstance(): SerializerInstance = ser.newInstance()
}

private[sql] object SparkSqlSerializer {
  @transient lazy val resourcePool = new KryoResourcePool(30)

  private[this] def acquireRelease[O](fn: SerializerInstance => O): O = {
    val kryo = resourcePool.borrow
    try {
      fn(kryo)
    } finally {
      resourcePool.release(kryo)
    }
  }

  def serialize[T: ClassTag](o: T): Array[Byte] =
    acquireRelease { k =>
      k.serialize(o).array()
    }

  def deserialize[T: ClassTag](bytes: Array[Byte]): T =
    acquireRelease { k =>
      k.deserialize[T](ByteBuffer.wrap(bytes))
    }
}

private[sql] class JavaBigDecimalSerializer extends Serializer[java.math.BigDecimal] {
  def write(kryo: Kryo, output: Output, bd: java.math.BigDecimal) {
    // TODO: There are probably more efficient representations than strings...
    output.writeString(bd.toString)
  }

  def read(kryo: Kryo, input: Input, tpe: Class[java.math.BigDecimal]): java.math.BigDecimal = {
    new java.math.BigDecimal(input.readString())
  }
}

private[sql] class ScalaBigDecimalSerializer extends Serializer[BigDecimal] {
  def write(kryo: Kryo, output: Output, bd: BigDecimal) {
    // TODO: There are probably more efficient representations than strings...
    output.writeString(bd.toString)
  }

  def read(kryo: Kryo, input: Input, tpe: Class[BigDecimal]): BigDecimal = {
    new java.math.BigDecimal(input.readString())
  }
}

private[sql] class HyperLogLogSerializer extends Serializer[HyperLogLog] {
  def write(kryo: Kryo, output: Output, hyperLogLog: HyperLogLog) {
    val bytes = hyperLogLog.getBytes
    output.writeInt(bytes.length)
    output.writeBytes(bytes)
  }

  def read(kryo: Kryo, input: Input, tpe: Class[HyperLogLog]): HyperLogLog = {
    val length = input.readInt()
    val bytes = input.readBytes(length)
    HyperLogLog.Builder.build(bytes)
  }
}

private[sql] class OpenHashSetSerializer extends Serializer[OpenHashSet[_]] {
  def write(kryo: Kryo, output: Output, hs: OpenHashSet[_]) {
    val rowSerializer = kryo.getDefaultSerializer(classOf[Array[Any]]).asInstanceOf[Serializer[Any]]
    output.writeInt(hs.size)
    val iterator = hs.iterator
    while(iterator.hasNext) {
      val row = iterator.next()
      rowSerializer.write(kryo, output, row.asInstanceOf[GenericInternalRow].values)
    }
  }

  def read(kryo: Kryo, input: Input, tpe: Class[OpenHashSet[_]]): OpenHashSet[_] = {
    val rowSerializer = kryo.getDefaultSerializer(classOf[Array[Any]]).asInstanceOf[Serializer[Any]]
    val numItems = input.readInt()
    val set = new OpenHashSet[Any](numItems + 1)
    var i = 0
    while (i < numItems) {
      val row =
        new GenericInternalRow(rowSerializer.read(
          kryo,
          input,
          classOf[Array[Any]].asInstanceOf[Class[Any]]).asInstanceOf[Array[Any]])
      set.add(row)
      i += 1
    }
    set
  }
}