package org.gradoop.common.properties.strategies

import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.Type

import scala.collection.{Map, mutable}

object MapStrategy extends VariableSizedPropertyValueStrategy[Map[PropertyValue, PropertyValue]] {

  override def putRawBytes(bytes: Array[Byte], offset: Int, value: Map[PropertyValue, PropertyValue]): Unit = {
    val it = value.iterator
    var index = offset
    while (it.nonEmpty) {
      val entry = it.next()
      Bytes.putBytes(bytes, index, entry._1.value, 0, entry._1.value.length)
      index += entry._1.value.length
      Bytes.putBytes(bytes, index, entry._2.value, 0, entry._2.value.length)
      index += entry._2.value.length
    }
  }

  override def fromRawBytes(bytes: Array[Byte], offset: Int, size: Int): Map[PropertyValue, PropertyValue] = {
    var index = offset
    var seq = Seq.empty[Tuple2[PropertyValue, PropertyValue]]
    while (index < offset + size) {
      val key = PropertyValue(PropertyValueStrategy(bytes(index)).fromBytes(bytes, index))
      index += key.value.length
      val value = PropertyValue(PropertyValueStrategy(bytes(index)).fromBytes(bytes, index))
      index += value.value.length
      seq :+= (key, value)
    }
    seq.toMap[PropertyValue, PropertyValue]
  }

  override def compare(value: Map[PropertyValue, PropertyValue], other: Any): Int = {
    throw new UnsupportedOperationException("Method compare() is not supported for Map.")
  }

  override def is(value: Any): Boolean = {
    value.isInstanceOf[Map[_, _]] && value.asInstanceOf[Map[_, _]]
      .forall(e => e._1.isInstanceOf[PropertyValue] && e._2.isInstanceOf[PropertyValue])
  }

  override def getRawSize(value: Map[PropertyValue, PropertyValue]): Int = {
    value.map(e => e._1.value.length + e._2.value.length).sum
  }

  override def getType: Type = Type.MAP

  override def getExactType(value: Map[PropertyValue, PropertyValue]): Type = {
    if(value.isEmpty) {
      Type.TYPED_MAP(Type.NULL, Type.NULL)
    } else {
      Type.TYPED_MAP(value.head._1.getExactType, value.head._2.getExactType)
    }
  }
}
