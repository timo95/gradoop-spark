package org.gradoop.common.properties.strategies2

import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.properties.{PropertyValue, Type}

object ListStrategy extends VariableSizedPropertyValueStrategy[List[PropertyValue]] {

  override def putRawBytes(bytes: Array[Byte], offset: Int, value: List[PropertyValue]): Unit = {
    val it = value.iterator
    var index = offset
    while (it.nonEmpty) {
      val prop = it.next()
      Bytes.putBytes(bytes, index, prop.value, 0, prop.value.length)
      index += prop.value.length
    }
  }

  override def fromRawBytes(bytes: Array[Byte], offset: Int, size: Int): List[PropertyValue] = {
    var index = offset
    var seq = Seq.empty[PropertyValue]
    while (index < offset + size) {
      val element = PropertyValue(PropertyValueStrategy(bytes(index)).fromBytes(bytes, index))
      index += element.value.length
      seq :+= element
    }
    seq.toList
  }

  override def compare(value: List[PropertyValue], other: Any): Int = {
    throw new UnsupportedOperationException("Method compare() is not supported for List.")
  }

  override def is(value: Any): Boolean = {
    value.isInstanceOf[List[_]] && value.asInstanceOf[List[_]].forall(_.isInstanceOf[PropertyValue])
  }

  override def getRawSize(value: List[PropertyValue]): Int = value.map(_.value.length).sum

  override def getType: Type = Type.LIST

  override def getExactType(value: List[PropertyValue]): Type = {
    val innerType = if(value.isEmpty) Type.NULL else value.head.getExactType
    Type.TYPED_LIST(innerType)
  }
}
