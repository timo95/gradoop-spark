package org.gradoop.common.properties

import org.gradoop.common.util.Type

object PropertyValueUtils {

  /**
   * Short type.
   */
  private val SHORT = 0
  /**
   * Integer type.
   */
  private val INT = 1
  /**
   * Long type.
   */
  private val LONG = 2
  /**
   * Float type.
   */
  private val FLOAT = 3
  /**
   * Double type.
   */
  private val DOUBLE = 4
  /**
   * Big decimal type.
   */
  private val BIG_DECIMAL = 5

  /**
   * Returns the maximum of two types, at least Integer.
   *
   * @param aType first type
   * @param bType second type
   * @return larger compatible type
   */
  private def maxType(aType: Int, bType: Int) = Math.max(Math.max(aType, bType), INT)

  /**
   * Adds two numerical property values.
   *
   * @param aValue first value
   * @param bValue second value
   * @return first value + second value
   */
  def add(aValue: PropertyValue, bValue: PropertyValue): PropertyValue = {
    val aType = checkNumericalAndGetType(aValue)
    val bType = checkNumericalAndGetType(bValue)
    val returnType = maxType(aType, bType)

    returnType match {
      case INT => PropertyValue(aValue.getNumber.intValue + bValue.getNumber.intValue)
      case FLOAT => PropertyValue(aValue.getNumber.floatValue + bValue.getNumber.floatValue)
      case LONG => PropertyValue(aValue.getNumber.longValue + bValue.getNumber.longValue)
      case DOUBLE => PropertyValue(aValue.getNumber.doubleValue + bValue.getNumber.doubleValue)
      case BIG_DECIMAL => PropertyValue(bigDecimalValue(aValue, aType) + bigDecimalValue(bValue, bType))
      case _ => throw new IllegalArgumentException("Unknown type with number " + returnType)
    }
  }

  /**
   * Multiplies two numerical property values.
   *
   * @param aValue first value
   * @param bValue second value
   * @return first value * second value
   */
  def multiply(aValue: PropertyValue, bValue: PropertyValue): PropertyValue = {
    val aType = checkNumericalAndGetType(aValue)
    val bType = checkNumericalAndGetType(bValue)
    val returnType = maxType(aType, bType)

    returnType match {
      case INT => PropertyValue(aValue.getNumber.intValue * bValue.getNumber.intValue)
      case FLOAT => PropertyValue(aValue.getNumber.floatValue * bValue.getNumber.floatValue)
      case LONG => PropertyValue(aValue.getNumber.longValue * bValue.getNumber.longValue)
      case DOUBLE => PropertyValue(aValue.getNumber.doubleValue * bValue.getNumber.doubleValue)
      case BIG_DECIMAL => PropertyValue(bigDecimalValue(aValue, aType) * bigDecimalValue(bValue, bType))
      case _ => throw new IllegalArgumentException("Unknown type with number " + returnType)
    }
  }

  /**
   * Compares two numerical property values
   *
   * @param aValue first value
   * @param bValue second value
   * @return 0 if a equal to b, { @code < 0} if { @code a < b} and { @code > 0} if { @code a > b}
   */
  def compare(aValue: PropertyValue, bValue: PropertyValue): Int = {
    val aType = checkNumericalAndGetType(aValue)
    val bType = checkNumericalAndGetType(bValue)
    val maxType = Math.max(aType, bType)

    maxType match {
      case SHORT => aValue.getShort.compareTo(bValue.getShort)
      case INT => aValue.getNumber.intValue.compareTo(bValue.getNumber.intValue)
      case FLOAT => aValue.getNumber.floatValue.compareTo(bValue.getNumber.floatValue)
      case LONG => aValue.getNumber.longValue.compareTo(bValue.getNumber.longValue)
      case DOUBLE => aValue.getNumber.doubleValue.compareTo(bValue.getNumber.doubleValue)
      case BIG_DECIMAL => bigDecimalValue(aValue, aType).compareTo(bigDecimalValue(bValue, bType))
      case _ => throw new IllegalArgumentException("Unknown type with number " + maxType)
    }
  }

  /**
   * Compares two numerical property values and returns true,
   * if the first one is smaller.
   *
   * @param aValue first value
   * @param bValue second value
   * @return a < b
   */
  private def isLessOrEqualThan(aValue: PropertyValue, bValue: PropertyValue) = {
    val aType = checkNumericalAndGetType(aValue)
    val bType = checkNumericalAndGetType(bValue)
    val returnType = maxType(aType, bType)

    returnType match {
      case INT => aValue.getNumber.intValue <= bValue.getNumber.intValue
      case FLOAT => aValue.getNumber.floatValue <= bValue.getNumber.floatValue
      case LONG => aValue.getNumber.longValue <= bValue.getNumber.longValue
      case DOUBLE => aValue.getNumber.doubleValue <= bValue.getNumber.doubleValue
      case BIG_DECIMAL => bigDecimalValue(aValue, aType) <= bigDecimalValue(bValue, bType)
      case _ => throw new IllegalArgumentException("Unknown type with number " + returnType)
    }
  }

  /**
   * Compares two numerical property values and returns the smaller one.
   *
   * @param a first value
   * @param b second value
   * @return smaller value
   */
  def min(a: PropertyValue, b: PropertyValue): PropertyValue = if (isLessOrEqualThan(a, b)) a else b

  /**
   * Compares two numerical property values and returns the bigger one.
   *
   * @param a first value
   * @param b second value
   * @return bigger value
   */
  def max(a: PropertyValue, b: PropertyValue): PropertyValue = if (isLessOrEqualThan(a, b)) b else a

  /**
   * Checks a property value for numerical type and returns its type.
   *
   * @param value property value
   * @return numerical type
   */
  private def checkNumericalAndGetType(value: PropertyValue) = value.getTypeByte match {
    case Type.SHORT.byte => SHORT
    case Type.INTEGER.byte => INT
    case Type.LONG.byte => LONG
    case Type.FLOAT.byte => FLOAT
    case Type.DOUBLE.byte => DOUBLE
    case Type.BIG_DECIMAL.byte => BIG_DECIMAL
    case _ => throw new IllegalArgumentException("Type not supported: " + value.getType.string)
  }

  /**
   * Converts a value of a lower domain numerical type to BigDecimal.
   *
   * @param value value
   * @param typeNum  type
   * @return converted value
   */
  private def bigDecimalValue(value: PropertyValue, typeNum: Int) = typeNum match {
    case BIG_DECIMAL => value.getBigDecimal
    case SHORT => BigDecimal.valueOf(value.getShort)
    case INT => BigDecimal.valueOf(value.getInt)
    case LONG => BigDecimal.valueOf(value.getLong)
    case FLOAT => BigDecimal.valueOf(value.getFloat.toDouble)
    case DOUBLE => BigDecimal.valueOf(value.getDouble)
    case _ => throw new IllegalArgumentException("Unknown type with number " + typeNum)
  }
}
