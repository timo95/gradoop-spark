package org.gradoop.common.properties

import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.properties.CompoundType.TYPE_TOKEN_DELIMITER
import org.gradoop.common.properties.Type._

sealed abstract class Type {
  def string: String
  def byte: Byte
  def getTypeClass: Class[_] = classFromType(this)

  def getTypeByte: Byte = byte // to support java
}

sealed abstract class PrimitiveType(val string: String, val byte: Byte) extends Type

sealed abstract class CompoundType(val string: String, val mainType: PrimitiveType) extends Type {
  override def byte: Byte = mainType.byte
}

object Type {
  // Primitive types
  case object NULL extends PrimitiveType("null", 0x00)
  case object BOOLEAN extends PrimitiveType("boolean", 0x01)
  case object INTEGER extends PrimitiveType("int", 0x02)
  case object LONG extends PrimitiveType("long", 0x03)
  case object FLOAT extends PrimitiveType("float", 0x04)
  case object DOUBLE extends PrimitiveType("double", 0x05)
  case object STRING extends PrimitiveType("string", 0x06)
  case object BIG_DECIMAL extends PrimitiveType("bigdecimal", 0x07)
  case object GRADOOP_ID extends PrimitiveType("gradoopid", 0x08)
  case object MAP extends PrimitiveType("map", 0x09)
  case object LIST extends PrimitiveType("list", 0x0a)
  case object DATE extends PrimitiveType("localdate", 0x0b)
  case object TIME extends PrimitiveType("localtime", 0x0c)
  case object DATE_TIME extends PrimitiveType("localdatetime", 0x0d)
  case object SHORT extends PrimitiveType("short", 0x0e)
  case object SET extends PrimitiveType("set", 0x0f)

  // Compound types
  case class TYPED_LIST(elementType: Type)
    extends CompoundType(LIST.string + TYPE_TOKEN_DELIMITER + elementType.string, LIST)
  case class TYPED_SET(elementType: Type)
    extends CompoundType(SET.string + TYPE_TOKEN_DELIMITER + elementType.string, SET)
  case class TYPED_MAP(keyType: Type, valueType: Type)
    extends CompoundType(MAP.string + TYPE_TOKEN_DELIMITER + keyType.string + TYPE_TOKEN_DELIMITER + valueType.string, MAP)

  /** Type from type string */
  def apply(typeString: String): Type = {
    typeString.toLowerCase match {
      case Type.NULL.string => Type.NULL
      case Type.BOOLEAN.string => Type.BOOLEAN
      case Type.INTEGER.string => Type.INTEGER
      case Type.LONG.string => Type.LONG
      case Type.FLOAT.string => Type.FLOAT
      case Type.DOUBLE.string => Type.DOUBLE
      case Type.STRING.string => Type.STRING
      case Type.BIG_DECIMAL.string => Type.BIG_DECIMAL
      case Type.GRADOOP_ID.string => Type.GRADOOP_ID
      case Type.MAP.string => Type.MAP
      case Type.LIST.string => Type.LIST
      case Type.DATE.string => Type.DATE
      case Type.TIME.string => Type.TIME
      case Type.DATE_TIME.string => Type.DATE_TIME
      case Type.SHORT.string => Type.SHORT
      case Type.SET.string => Type.SET
      case _ => CompoundType(typeString)
    }
  }

  /** Type from type byte */
  def apply(typeByte: Byte): Type = {
    typeByte match {
      case Type.NULL.byte => Type.NULL
      case Type.BOOLEAN.byte => Type.BOOLEAN
      case Type.INTEGER.byte => Type.INTEGER
      case Type.LONG.byte => Type.LONG
      case Type.FLOAT.byte => Type.FLOAT
      case Type.DOUBLE.byte => Type.DOUBLE
      case Type.STRING.byte => Type.STRING
      case Type.BIG_DECIMAL.byte => Type.BIG_DECIMAL
      case Type.GRADOOP_ID.byte => Type.GRADOOP_ID
      case Type.MAP.byte => Type.MAP
      case Type.LIST.byte => Type.LIST
      case Type.DATE.byte => Type.DATE
      case Type.TIME.byte => Type.TIME
      case Type.DATE_TIME.byte => Type.DATE_TIME
      case Type.SHORT.byte => Type.SHORT
      case Type.SET.byte => Type.SET
      case _ => throw new IllegalArgumentException("Type could not be found: " + typeByte)
    }
  }

  /** Class from Type */
  def classFromType(typ: Type): Class[_] = {
    typ match {
      case NULL => null // TODO exchange for Null/Nothing etc type
      case BOOLEAN => classOf[Boolean]
      case INTEGER => classOf[Int]
      case LONG => classOf[Long]
      case FLOAT => classOf[Float]
      case DOUBLE => classOf[Double]
      case STRING => classOf[String]
      case BIG_DECIMAL => classOf[BigDecimal]
      case GRADOOP_ID => classOf[GradoopId]
      case MAP => classOf[Map[_, _]]
      case LIST => classOf[List[_]]
      case DATE => classOf[LocalDate]
      case TIME => classOf[LocalTime]
      case DATE_TIME => classOf[LocalDateTime]
      case SHORT => classOf[Short]
      case SET => classOf[Set[_]]
      case _ => throw new IllegalArgumentException("Type could not be found: " + typ)
    }
  }
}

object PrimitiveType {

  def of(obj: Any): Type = {
    obj match {
      case null => NULL
      case _: Boolean => BOOLEAN
      case _: Int => INTEGER
      case _: Long => LONG
      case _: Float => FLOAT
      case _: Double => DOUBLE
      case _: String => STRING
      case _: BigDecimal => BIG_DECIMAL
      case _: GradoopId => GRADOOP_ID
      case _: LocalDate => DATE
      case _: LocalTime => TIME
      case _: LocalDateTime => DATE_TIME
      case _: Short => SHORT
      case _: List[_] => LIST
      case _: Set[_] => SET
      case _: Map[_, _] => MAP
      case _ => throw new IllegalArgumentException("Type is not supported: " + obj.getClass.getSimpleName)
    }
  }
}

object CompoundType {
  /** Used to separate external type from internal types */
  val TYPE_TOKEN_DELIMITER = ':'

  def apply(typeString: String): CompoundType = {
    typeString match {
      case list: String if list.startsWith(LIST.string) => TYPED_LIST(Type(list.substring(LIST.string.length + 1)))
      case set: String if set.startsWith(SET.string) => TYPED_SET(Type(set.substring(SET.string.length + 1)))
      case map: String if map.startsWith(MAP.string) =>
        val tokens = map.split(TYPE_TOKEN_DELIMITER)
        TYPED_MAP(Type(tokens(1)), Type(tokens(2)))
      case _ => throw new IllegalArgumentException("Type could not be found: " + typeString)
    }
  }
}
