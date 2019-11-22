package org.gradoop.common.properties

import org.gradoop.common.properties.Type.{List, Map, Set, TypedList, TypedMap, TypedSet}

sealed abstract class Type {
  def string: String
  def byte: Byte
}

sealed abstract class PrimitiveType(val string: String, val byte: Byte) extends Type

sealed abstract class CompoundType(val string: String, val mainType: Type) extends Type {
  override def byte: Byte = mainType.byte
}

object Type {
  // Primitive types
  case object Null extends PrimitiveType("null", 0x00)
  case object Boolean extends PrimitiveType("boolean", 0x01)
  case object Integer extends PrimitiveType("int", 0x02)
  case object Long extends PrimitiveType("long", 0x03)
  case object Float extends PrimitiveType("float", 0x04)
  case object Double extends PrimitiveType("double", 0x05)
  case object String extends PrimitiveType("string", 0x06)
  case object BigDecimal extends PrimitiveType("bigdecimal", 0x07)
  case object GradoopId extends PrimitiveType("gradoopid", 0x08)
  case object Map extends PrimitiveType("map", 0x09)
  case object List extends PrimitiveType("list", 0x0a)
  case object Date extends PrimitiveType("localdate", 0x0b)
  case object Time extends PrimitiveType("localtime", 0x0c)
  case object DateTime extends PrimitiveType("localdatetime", 0x0d)
  case object Short extends PrimitiveType("short", 0x0e)
  case object Set extends PrimitiveType("set", 0x0f)

  // Compound types
  case class TypedList(elementType: Type)
    extends CompoundType(s"${List.string}:${elementType.string}", List)
  case class TypedSet(elementType: Type)
    extends CompoundType(s"${Set.string}:${elementType.string}", Set)
  case class TypedMap(keyType: Type, valueType: Type)
    extends CompoundType(s"${Map.string}:${keyType.string}:${valueType.string}", Map)

  def apply(typeString: String): Type = {
    typeString.toLowerCase match {
      case Type.Null.string => Type.Null
      case Type.Boolean.string => Type.Boolean
      case Type.Integer.string => Type.Integer
      case Type.Long.string => Type.Long
      case Type.Float.string => Type.Float
      case Type.Double.string => Type.Double
      case Type.String.string => Type.String
      case Type.BigDecimal.string => Type.BigDecimal
      case Type.GradoopId.string => Type.GradoopId
      case Type.Map.string => Type.Map
      case Type.List.string => Type.List
      case Type.Date.string => Type.Date
      case Type.Time.string => Type.Time
      case Type.DateTime.string => Type.DateTime
      case Type.Short.string => Type.Short
      case Type.Set.string => Type.Set
      case _ => CompoundType(typeString)
    }
  }

  def apply(typeByte: Byte): Type = {
    typeByte match {
      case Type.Null.byte => Type.Null
      case Type.Boolean.byte => Type.Boolean
      case Type.Integer.byte => Type.Integer
      case Type.Long.byte => Type.Long
      case Type.Float.byte => Type.Float
      case Type.Double.byte => Type.Double
      case Type.String.byte => Type.String
      case Type.BigDecimal.byte => Type.BigDecimal
      case Type.GradoopId.byte => Type.GradoopId
      case Type.Map.byte => Type.Map
      case Type.List.byte => Type.List
      case Type.Date.byte => Type.Date
      case Type.Time.byte => Type.Time
      case Type.DateTime.byte => Type.DateTime
      case Type.Short.byte => Type.Short
      case Type.Set.byte => Type.Set
      case _ => throw new IllegalArgumentException("Type could not be found: " + typeByte)
    }
  }
}


object CompoundType {

  /** Used to separate external type from internal types */
  val TYPE_TOKEN_DELIMITER = ":"

  def apply(typeString: String): CompoundType = {
    typeString match {
      case list if list.startsWith(List.string) => TypedList(Type(list.substring(List.string.length + 1)))
      case set if set.startsWith(Set.string) => TypedSet(Type(set.substring(Set.string.length + 1)))
      case map if map.startsWith(Map.string) =>
        val tokens = map.split(TYPE_TOKEN_DELIMITER)
        TypedMap(Type(tokens(1)), Type(tokens(2)))
      case _ => throw new IllegalArgumentException("Type could not be found: " + typeString)
    }
  }
}