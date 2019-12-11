package org.gradoop.spark.io.impl.csv

import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.properties.{CompoundType, PropertyValue, Type}
import org.gradoop.spark.io.impl.metadata.ElementMetaData
import org.gradoop.spark.model.impl.types.Gve
import org.gradoop.spark.util.StringEscaper

abstract protected class CsvParser[L <: Gve[L]] extends Serializable {

  protected def parseId(idString: String): GradoopId = GradoopId.fromString(idString)

  protected def parseGraphIds(idSetString: String): IdSet = {
    idSetString.substring(1, idSetString.length - 1)
      .split(CsvConstants.LIST_DELIMITER)
      .map(GradoopId.fromString).toSet
  }

  protected def parseLabel(labelString: String): Label = {
    if(labelString == null) ""
    else StringEscaper.unescape(labelString)//StringEscaper.split(labelsString, GradoopConstants.LABEL_DELIMITER)
      //.map(StringEscaper.unescape)
  }

  protected def parseProperties(propertiesString: String, metaData: ElementMetaData): Properties = {
    if(propertiesString == null) Map.empty[String, PropertyValue]
    else {
      val properties = metaData.properties
      val propertyStrings = StringEscaper.split(propertiesString, CsvConstants.VALUE_DELIMITER)
      if(propertyStrings.length != properties.length)
        println(s"Number of Properties for '${metaData.label}' does not fit metadata." +
          s"Parsed Properties might be corrupt.") //TODO: Add logging
      val length = propertyStrings.length min properties.length // only parse when metadata exists for it
      (0 until length)
        .filter(i => !propertyStrings(i).equals("")) // empty value => no property value
        .map(i => (properties(i).key, propertyParser(properties(i).typeString)(propertyStrings(i))))
        .toMap
    }
  }

  private def propertyParser(typeString: String): String => PropertyValue = {
    valueString: String => PropertyValue(typeString match {
      case Type.NULL.string => null
      case Type.BOOLEAN.string => java.lang.Boolean.parseBoolean(valueString)
      case Type.INTEGER.string => java.lang.Integer.parseInt(valueString)
      case Type.LONG.string => java.lang.Long.parseLong(valueString)
      case Type.FLOAT.string => java.lang.Float.parseFloat(valueString)
      case Type.DOUBLE.string => java.lang.Double.parseDouble(valueString)
      case Type.STRING.string => StringEscaper.unescape(valueString)
      case Type.BIG_DECIMAL.string => BigDecimal(valueString)
      case Type.GRADOOP_ID.string => GradoopId.fromString(valueString)
      case Type.DATE.string => LocalDate.parse(valueString)
      case Type.TIME.string => LocalTime.parse(valueString)
      case Type.DATE_TIME.string => LocalDateTime.parse(valueString)
      case Type.SHORT.string => java.lang.Short.parseShort(valueString)
      case compound if compound.contains(CompoundType.TYPE_TOKEN_DELIMITER) => compoundParser(CompoundType(compound))(valueString)
      case _ => throw new IllegalArgumentException("Type not yet supported: " + typeString)
    })
  }

  private def compoundParser(compoundType: CompoundType): String => Iterable[_] = {
    valueString: String => compoundType match {
      case Type.TYPED_LIST(elementType) => arrayParser(propertyParser(elementType.string))(valueString).toList
      case Type.TYPED_SET(elementType) => arrayParser(propertyParser(elementType.string))(valueString).toSet
      case Type.TYPED_MAP(keyType, valueType) =>
        mapParser(propertyParser(keyType.string), propertyParser(valueType.string))(valueString)
      case _ => throw new IllegalArgumentException("Type not yet supported: " + compoundType.string)
    }
  }

  private def arrayParser(elementParser: String => PropertyValue): String => Array[PropertyValue] = {
    arrayString: String => StringEscaper.split(arrayString.substring(1, arrayString.length - 1), CsvConstants.LIST_DELIMITER)
      .map(elementParser)
  }

  private def mapParser(keyParser: String => PropertyValue, valueParser: String => PropertyValue):
  String => Map[PropertyValue, PropertyValue] = {
    mapString: String => StringEscaper.split(mapString.substring(1, mapString.length - 1), CsvConstants.LIST_DELIMITER)
      .map(entry => StringEscaper.split(entry, CsvConstants.MAP_SEPARATOR, 2))
      .map(entry => (PropertyValue(entry(0)), PropertyValue(entry(1))))
      .toMap
  }
}
