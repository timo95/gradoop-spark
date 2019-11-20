package org.gradoop.common.properties

import org.gradoop.common.properties.Type.{List, Map, Set, TYPE_TOKEN_DELIMITER}

sealed abstract class Type(val string: String, val byte: Byte)

object Type {

  /** Used to separate external type from internal types */
  val TYPE_TOKEN_DELIMITER = ":"

  case object Null extends Type("null", 0x00)
  case object Boolean extends Type("boolean", 0x01)
  case object Integer extends Type("int", 0x02)
  case object Long extends Type("long", 0x03)
  case object Float extends Type("float", 0x04)
  case object Double extends Type("double", 0x05)
  case object String extends Type("string", 0x06)
  case object BigDecimal extends Type("bigdecimal", 0x07)
  case object GradoopId extends Type("gradoopid", 0x08)
  case object Map extends Type("map", 0x09)
  case object List extends Type("list", 0x0a)
  case object Date extends Type("localdate", 0x0b)
  case object Time extends Type("localtime", 0x0c)
  case object DateTime extends Type("localdatetime", 0x0d)
  case object Short extends Type("short", 0x0e)
  case object Set extends Type("set", 0x0f)

  def apply(typeString: String): Type = { // TODO for complex types
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
      case _ => ComplexType(typeString)
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

sealed abstract class ComplexType(string: String, val mainType: Type) extends Type(string, mainType.byte)

object ComplexType {

  // Complex types
  case class TypedList(elementType: Type) extends ComplexType(s"${List.string}:${elementType.string}", List)
  case class TypedSet(elementType: Type) extends ComplexType(s"${Set.string}:${elementType.string}", Set)
  case class TypedMap(keyType: Type, valueType: Type) extends ComplexType(s"${Map.string}:${keyType.string}:${valueType.string}", Map)

  def apply(typeString: String): ComplexType = {
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