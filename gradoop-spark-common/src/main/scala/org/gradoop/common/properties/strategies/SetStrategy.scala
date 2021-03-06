package org.gradoop.common.properties.strategies

import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.Type

object SetStrategy extends VariableSizedPropertyValueStrategy[Set[PropertyValue]] {

  override def putRawBytes(bytes: Array[Byte], offset: Int, value: Set[PropertyValue]): Unit = {
    val it = value.iterator
    var index = offset
    while (it.nonEmpty) {
      val prop = it.next()
      Bytes.putBytes(bytes, index, prop.bytes, 0, prop.bytes.length)
      index += prop.bytes.length
    }
  }

  override def fromRawBytes(bytes: Array[Byte], offset: Int, size: Int): Set[PropertyValue] = {
    var index = offset
    var seq = Seq.empty[PropertyValue]
    while (index < offset + size) {
      val element = PropertyValue(PropertyValueStrategy(bytes(index)).fromBytes(bytes, index))
      index += element.bytes.length
      seq :+= element
    }
    seq.toSet
  }

  override def compare(value: Set[PropertyValue], other: Any): Int = {
    throw new UnsupportedOperationException("Method compare() is not supported for Set.")
  }

  override def is(value: Any): Boolean = {
    value.isInstanceOf[Set[_]] && value.asInstanceOf[Set[_]].forall(_.isInstanceOf[PropertyValue])
  }

  override def getRawSize(value: Set[PropertyValue]): Int = value.toArray.map(_.bytes.length).sum

  override def getType: Type = Type.SET

  override def getExactType(value: Set[PropertyValue]): Type = {
    val innerType = if(value.isEmpty) Type.NULL else value.head.getExactType
    Type.TYPED_SET(innerType)
  }
}
