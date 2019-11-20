package org.gradoop.spark.io.impl.csv

import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.properties.{ComplexType, PropertyValue, Type}
import org.gradoop.spark.io.impl.metadata.ElementMetaData
import org.gradoop.spark.model.impl.types.GveLayoutType
import org.gradoop.spark.util.StringEscaper

abstract protected class CsvParser[L <: GveLayoutType] extends Serializable {

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
    if(propertiesString == null) Map.empty[String, PV]
    else {
      val properties = metaData.properties
      val propertyStrings = StringEscaper.split(propertiesString, CsvConstants.VALUE_DELIMITER)
      if(propertyStrings.length != properties.length) println(s"Number of Properties for '${metaData.label}' does not fit metadata. Parsed Properties might be corrupt.") //TODO: Add logging
      val length = propertyStrings.length min properties.length // only parse when metadata exists for it
      (0 until length)
        .filter(i => !propertyStrings(i).equals("")) // empty value => no property value
        .map(i => (properties(i).key, parsePropertyValue(propertyStrings(i), properties(i).typeString)))
        .toMap
    }
  }

  private def parsePropertyValue(valueString: String, typeString: String): PropertyValue = {
    PropertyValue(typeString match {
      case Type.Null.string => null
      case Type.Boolean.string => java.lang.Boolean.parseBoolean(valueString)
      case Type.Integer.string => java.lang.Integer.parseInt(valueString)
      case Type.Long.string => java.lang.Long.parseLong(valueString)
      case Type.Float.string => java.lang.Float.parseFloat(valueString)
      case Type.Double.string => java.lang.Double.parseDouble(valueString)
      case Type.String.string => StringEscaper.unescape(valueString)
      case Type.BigDecimal.string => BigDecimal(valueString)
      case Type.GradoopId.string => GradoopId.fromString(valueString)
      case Type.Date.string => LocalDate.parse(valueString)
      case Type.Time.string => LocalTime.parse(valueString)
      case Type.DateTime.string => LocalDateTime.parse(valueString)
      case Type.Short.string => java.lang.Short.parseShort(valueString)
      case list if list.startsWith(Type.List.string) => complexParser(valueString, ComplexType(list))
      case set if set.startsWith(Type.Set.string) => complexParser(valueString, ComplexType(set))
      case map if map.startsWith(Type.Map.string) => complexParser(valueString, ComplexType(map))
      case _ => throw new IllegalArgumentException("Type not yet supported: " + typeString)
    })
  }

  private def complexParser(valueString: String, complexType: ComplexType): PropertyValue = {
    PropertyValue(valueString) // TODO parse
  }
}
