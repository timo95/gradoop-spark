package org.gradoop.spark.io.impl.csv

import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.Type
import org.gradoop.common.util.Type.CompoundType
import org.gradoop.spark.util.StringEscaper

object CsvParser extends Serializable {

  def parseId(idString: String): GradoopId = GradoopId.fromString(idString)

  def parseGraphIds(idSetString: String): Array[GradoopId] = {
    idSetString.substring(1, idSetString.length - 1)
      .split(CsvConstants.LIST_DELIMITER)
      .map(GradoopId.fromString)
  }

  def parseLabel(labelString: String): Label = {
    if(labelString == null) ""
    else StringEscaper.unescape(labelString)
  }

  def parseProperties(propertiesString: String, label: String, metaData: Seq[GenericRowWithSchema]): Seq[(String, PropertyValue)] = {
    if(propertiesString == null) Seq.empty[(String, PropertyValue)]
    else {
      val propertyStrings = StringEscaper.split(propertiesString, CsvConstants.VALUE_DELIMITER)
      if(propertyStrings.length != metaData.length) {
        println(s"Number of Properties for '${label}' does not fit metadata." +
          s"Parsed Properties might be corrupt.")
      }

      val length = propertyStrings.length min metaData.length // only parse when metadata exists for it
      (0 until length)
        .filter(i => !propertyStrings(i).equals("")) // empty value => no property value
        .map(i => {
          val key = metaData(i)(0).asInstanceOf[String] // workaround for weird bug, cannot cast to PropertyMetaData
          val typeString = metaData(i)(1).asInstanceOf[String] // TODO build minimal example (struct + udf)
          (key, propertyParser(typeString)(propertyStrings(i)))
        })
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
      case compound: String if compound.contains(CompoundType.TYPE_TOKEN_DELIMITER) => compoundParser(CompoundType(compound))(valueString)
      case _ => throw new IllegalArgumentException("Type not yet supported: " + typeString)
    })
  }

  private def compoundParser(compoundType: CompoundType): String => Iterable[_] = {
    valueString: String => compoundType match {
      case Type.TYPED_LIST(elementType) => arrayParser(propertyParser(elementType.string))(valueString).toSeq
      case Type.TYPED_SET(elementType) => arrayParser(propertyParser(elementType.string))(valueString).toSet
      case Type.TYPED_MAP(keyType, valueType) =>
        mapParser(propertyParser(keyType.string), propertyParser(valueType.string))(valueString)
      case _ => throw new IllegalArgumentException("Type not yet supported: " + compoundType.string)
    }
  }

  private def arrayParser(elementParser: String => PropertyValue): String => Array[PropertyValue] = {
    arrayString: String => StringEscaper.split(arrayString.substring(1, arrayString.length - 1),
      CsvConstants.LIST_DELIMITER).map(elementParser)
  }

  private def mapParser(keyParser: String => PropertyValue, valueParser: String => PropertyValue):
  String => Map[PropertyValue, PropertyValue] = {
    mapString: String => StringEscaper.split(mapString.substring(1, mapString.length - 1), CsvConstants.LIST_DELIMITER)
      .map(entry => StringEscaper.split(entry, CsvConstants.MAP_SEPARATOR, 2))
      .map(entry => (PropertyValue(entry(0)), PropertyValue(entry(1))))
      .toMap
  }
}
