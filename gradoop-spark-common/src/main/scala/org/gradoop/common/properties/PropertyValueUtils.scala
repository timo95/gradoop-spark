package org.gradoop.common.properties

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
    val sameType = aType == bType
    val returnType = maxType(aType, bType)
    if (returnType == INT) {
      val a = if (aType == INT) aValue.getInt
      else aValue.getShort
      val b = if (bType == INT) bValue.getInt
      else bValue.getShort
      PropertyValue(a + b)
    }
    else if (returnType == FLOAT) {
      var a = .0
      var b = .0
      if (sameType) {
        a = aValue.getFloat
        b = bValue.getFloat
      }
      else {
        a = if (aType == FLOAT) aValue.getFloat
        else aValue.getNumber.floatValue
        b = if (bType == FLOAT) bValue.getFloat
        else bValue.getNumber.floatValue
      }
      PropertyValue(a + b)
    }
    else if (returnType == LONG) {
      var a = 0L
      var b = 0L
      if (sameType) {
        a = aValue.getLong
        b = bValue.getLong
      }
      else {
        a = if (aType == LONG) aValue.getLong
        else aValue.getNumber.longValue
        b = if (bType == LONG) bValue.getLong
        else bValue.getNumber.longValue
      }
      PropertyValue(a + b)
    }
    else if (returnType == DOUBLE) {
      var a = .0
      var b = .0
      if (sameType) {
        a = aValue.getDouble
        b = bValue.getDouble
      }
      else {
        a = if (aType == DOUBLE) aValue.getDouble
        else aValue.getNumber.doubleValue
        b = if (bType == DOUBLE) bValue.getDouble
        else bValue.getNumber.doubleValue
      }
      PropertyValue(a + b)
    }
    else {
      if (sameType) {
        val a = aValue.getBigDecimal
        val b = bValue.getBigDecimal
        PropertyValue(a + b)
      }
      else {
        val a = if (aType == BIG_DECIMAL) aValue.getBigDecimal
        else bigDecimalValue(aValue, aType)
        val b = if (bType == BIG_DECIMAL) bValue.getBigDecimal
        else bigDecimalValue(bValue, bType)
        PropertyValue(a + b)
      }
    }
  }


  /**
   * Checks a property value for numerical type and returns its type.
   *
   * @param value property value
   * @return numerical type
   */
  private def checkNumericalAndGetType(value: PropertyValue) = {
    var typeNum = 0
    if (value.isShort) typeNum = SHORT
    else if (value.isInt) typeNum = INT
    else if (value.isLong) typeNum = LONG
    else if (value.isFloat) typeNum = FLOAT
    else if (value.isDouble) typeNum = DOUBLE
    else if (value.isBigDecimal) typeNum = BIG_DECIMAL
    else throw new IllegalArgumentException("Type not supported: " + value.getType.string)
    typeNum
  }

  /**
   * Converts a value of a lower domain numerical type to BigDecimal.
   *
   * @param value value
   * @param typeNum  type
   * @return converted value
   */
  private def bigDecimalValue(value: PropertyValue, typeNum: Int) = typeNum match {
    case SHORT =>
      BigDecimal.valueOf(value.getShort)
    case INT =>
      BigDecimal.valueOf(value.getInt)
    case LONG =>
      BigDecimal.valueOf(value.getLong)
    case FLOAT =>
      BigDecimal.valueOf(value.getFloat.toDouble)
    case _ =>
      BigDecimal.valueOf(value.getDouble)
  }
}
