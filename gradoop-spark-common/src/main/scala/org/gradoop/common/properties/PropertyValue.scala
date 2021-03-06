package org.gradoop.common.properties

import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.strategies.PropertyValueStrategy
import org.gradoop.common.util.Type.PrimitiveType
import org.gradoop.common.util.{GradoopConstants, Type}

case class PropertyValue(bytes: Array[Byte]) {

  def copy: PropertyValue = new PropertyValue(bytes.clone)

  def getTypeByte: Byte = bytes(0)

  def getType: Type = Type(getTypeByte)

  def getExactType: Type = PropertyValueStrategy(getTypeByte).getExactType(bytes)

  def get: Any = PropertyValueStrategy(getTypeByte).fromBytes(bytes)

  override def equals(o: Any): Boolean = {
    o match {
      case prop: PropertyValue => bytes.sameElements(prop.bytes)
      case _ => false
    }
  }

  override def hashCode(): Int = bytes.hashCode()

  override def toString: String = {
    val value = get
    val string = if(value == null) GradoopConstants.NULL_STRING else value.toString
    "%s:%s".format(string, getExactType.string)
  }

  // ---------- Convenience accessors ----------
  // Getter
  def getBoolean: Boolean = get.ensuring(_.isInstanceOf[Boolean]).asInstanceOf[Boolean]

  def getShort: Short = get.ensuring(_.isInstanceOf[Short]).asInstanceOf[Short]

  def getInt: Int = get.ensuring(_.isInstanceOf[Int]).asInstanceOf[Int]

  def getLong: Long = get.ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]

  def getFloat: Float = get.ensuring(_.isInstanceOf[Float]).asInstanceOf[Float]

  def getDouble: Double = get.ensuring(_.isInstanceOf[Double]).asInstanceOf[Double]

  def getString: String = get.ensuring(_.isInstanceOf[String]).asInstanceOf[String]

  def getBigDecimal: BigDecimal = get.ensuring(_.isInstanceOf[BigDecimal]).asInstanceOf[BigDecimal]

  def getGradoopId: GradoopId = get.ensuring(_.isInstanceOf[GradoopId]).asInstanceOf[GradoopId]

  def getDate: LocalDate = get.ensuring(_.isInstanceOf[LocalDate]).asInstanceOf[LocalDate]

  def getTime: LocalTime = get.ensuring(_.isInstanceOf[LocalTime]).asInstanceOf[LocalTime]

  def getDateTime: LocalDateTime = get.ensuring(_.isInstanceOf[LocalDateTime]).asInstanceOf[LocalDateTime]

  def getList: Seq[PropertyValue] = get.ensuring(_.isInstanceOf[Seq[_]]).asInstanceOf[Seq[PropertyValue]]

  def getSet: Set[PropertyValue] = get.ensuring(_.isInstanceOf[Set[_]]).asInstanceOf[Set[PropertyValue]]

  def getMap: Map[PropertyValue, PropertyValue] = get.ensuring(_.isInstanceOf[Map[_, _]])
    .asInstanceOf[Map[PropertyValue, PropertyValue]]

  // Special getter
  def getNumber: Number = get.ensuring(_.isInstanceOf[Number]).asInstanceOf[Number]

  // is type
  def isBoolean: Boolean = getTypeByte == Type.BOOLEAN.byte

  def isShort: Boolean = getTypeByte == Type.SHORT.byte

  def isInt: Boolean = getTypeByte == Type.INTEGER.byte

  def isLong: Boolean = getTypeByte == Type.LONG.byte

  def isFloat: Boolean = getTypeByte == Type.FLOAT.byte

  def isDouble: Boolean = getTypeByte == Type.DOUBLE.byte

  def isString: Boolean = getTypeByte == Type.STRING.byte

  def isBigDecimal: Boolean = getTypeByte == Type.BIG_DECIMAL.byte

  def isGradoopId: Boolean = getTypeByte == Type.GRADOOP_ID.byte

  def isDate: Boolean = getTypeByte == Type.DATE.byte

  def isTime: Boolean = getTypeByte == Type.TIME.byte

  def isDateTime: Boolean = getTypeByte == Type.DATE_TIME.byte

  def isList: Boolean = getTypeByte == Type.LIST.byte

  def isSet: Boolean = getTypeByte == Type.SET.byte

  def isMap: Boolean = getTypeByte == Type.MAP.byte
}

object PropertyValue {
  val bytes = "bytes"

  /** Value offset in byte */
  val OFFSET: Byte = 0x01.toByte

  /** Bit flag indicating a "large" property. The length of the byte representation will be stored as an {@code int} instead. */
  val FLAG_LARGE: Byte = 0x80.toByte

  /** If the length of the byte representation is larger than this value, FLAG_LARGE will be set. */
  val LARGE_PROPERTY_THRESHOLD: Int = Short.MaxValue

  val NULL_VALUE = new PropertyValue(Array(Type.NULL.byte))

  def apply[A](value: A): PropertyValue = {
    val strategy = PropertyValueStrategy(PrimitiveType.of(value).byte)
      .asInstanceOf[PropertyValueStrategy[A]]
    if(!strategy.is(value)) {
      throw new IllegalArgumentException("Internal type of %s not supported.".format(value.getClass.getSimpleName))
    }
    new PropertyValue(strategy.getBytes(value))
  }
}
