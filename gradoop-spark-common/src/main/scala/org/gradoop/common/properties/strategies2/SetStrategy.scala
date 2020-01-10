package org.gradoop.common.properties.strategies2

import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.properties.{PropertyValue, Type}

import scala.collection.mutable

object SetStrategy extends VariableSizedPropertyValueStrategy[Set[PropertyValue]] {

  override def putRawBytes(bytes: Array[Byte], offset: Int, value: Set[PropertyValue]): Unit = {
    val it = value.iterator
    var index = offset
    while (it.nonEmpty) {
      val prop = it.next()
      Bytes.putBytes(bytes, index, prop.value, 0, prop.value.length)
      index += prop.value.length
    }
  }

  override def fromRawBytes(bytes: Array[Byte], offset: Int, size: Int): Set[PropertyValue] = {
    var index = offset
    val set = mutable.Set.empty[PropertyValue]
    while (index < offset + size) {
      val element = PropertyValue(PropertyValueStrategy(bytes(index)).fromBytes(bytes, index))
      index += element.value.length
      set.add(element)
    }
    set.toSet
  }

  override def compare(value: Set[PropertyValue], other: Any): Int = {
    throw new UnsupportedOperationException("Method compare() is not supported for Set.")
  }

  override def is(value: Any): Boolean = {
    value.isInstanceOf[Set[_]] && value.asInstanceOf[Set[_]].forall(_.isInstanceOf[PropertyValue])
  }

  override def getRawSize(value: Set[PropertyValue]): Int = value.map(_.value.length).sum

  override def getType: Type = Type.SET

  override def getExactType(value: Set[PropertyValue]): Type = {
    val innerType = if(value.isEmpty) Type.NULL else value.head.getExactType
    Type.TYPED_SET(innerType)
  }
}
