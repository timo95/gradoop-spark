package org.gradoop.common.properties.strategies

import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.Type

object ListStrategy extends VariableSizedPropertyValueStrategy[Seq[PropertyValue]] {

  override def putRawBytes(bytes: Array[Byte], offset: Int, value: Seq[PropertyValue]): Unit = {
    val it = value.iterator
    var index = offset
    while (it.nonEmpty) {
      val prop = it.next()
      Bytes.putBytes(bytes, index, prop.bytes, 0, prop.bytes.length)
      index += prop.bytes.length
    }
  }

  override def fromRawBytes(bytes: Array[Byte], offset: Int, size: Int): Seq[PropertyValue] = {
    var index = offset
    var seq = Seq.empty[PropertyValue]
    while (index < offset + size) {
      val element = PropertyValue(PropertyValueStrategy(bytes(index)).fromBytes(bytes, index))
      index += element.bytes.length
      seq :+= element
    }
    seq
  }

  override def compare(value: Seq[PropertyValue], other: Any): Int = {
    throw new UnsupportedOperationException("Method compare() is not supported for Seq.")
  }

  override def is(value: Any): Boolean = {
    value.isInstanceOf[Seq[_]] && value.asInstanceOf[Seq[_]].forall(_.isInstanceOf[PropertyValue])
  }

  override def getRawSize(value: Seq[PropertyValue]): Int = value.map(_.bytes.length).sum

  override def getType: Type = Type.LIST

  override def getExactType(value: Seq[PropertyValue]): Type = {
    val innerType = if(value.isEmpty) Type.NULL else value.head.getExactType
    Type.TYPED_LIST(innerType)
  }
}
