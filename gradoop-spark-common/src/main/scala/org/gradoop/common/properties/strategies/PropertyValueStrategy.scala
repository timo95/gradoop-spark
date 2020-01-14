package org.gradoop.common.properties.strategies

import org.gradoop.common.util.Type

trait PropertyValueStrategy[A] {

  def getBytes(value: A): Array[Byte]

  protected def putBytes(bytes: Array[Byte], offset: Int, value: A): Unit

  def fromBytes(bytes: Array[Byte]): A = fromBytes(bytes, 0)

  def fromBytes(bytes: Array[Byte], offset: Int): A

  def compare(value: A, other: Any): Int

  def is(value: Any): Boolean

  def getRawSize(value: A): Int

  def getSize(rawSize: Int): Int

  def getType: Type

  def getExactType(value: A): Type = getType

  def getExactType(bytes: Array[Byte]): Type = getExactType(fromBytes(bytes, 0))
}

object PropertyValueStrategy {

  def apply(typeByte: Byte): PropertyValueStrategy[_] = {
    typeByte match {
      case Type.NULL.byte => NullStrategy
      case Type.BOOLEAN.byte => BooleanStrategy
      case Type.INTEGER.byte => IntegerStrategy
      case Type.LONG.byte => LongStrategy
      case Type.FLOAT.byte => FloatStrategy
      case Type.DOUBLE.byte => DoubleStrategy
      case Type.STRING.byte => StringStrategy
      case Type.BIG_DECIMAL.byte => BigDecimalStrategy
      case Type.GRADOOP_ID.byte => GradoopIdStrategy
      case Type.MAP.byte => MapStrategy
      case Type.LIST.byte => ListStrategy
      case Type.DATE.byte => DateStrategy
      case Type.TIME.byte => TimeStrategy
      case Type.DATE_TIME.byte => DateTimeStrategy
      case Type.SHORT.byte => ShortStrategy
      case Type.SET.byte => SetStrategy
      case _ => throw new IllegalArgumentException("There is no strategy for type byte: " + typeByte)    }
  }
}
