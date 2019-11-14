package org.gradoop.spark.io.impl.csv

import org.gradoop.common.model.api.elements._
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.spark.util.StringEscaper

abstract protected class CsvParser[G <: GraphHead, V <: Vertex, E <: Edge](var metadata: Option[MetaData])
  extends Serializable {

  protected def parseId(idString: String): GradoopId = GradoopId.fromString(idString)

  protected def parseGraphIds(idSetString: String): IdSet = {
    idSetString.substring(1, idSetString.length - 1)
      .split(CsvConstants.LIST_DELIMITER)
      .map(GradoopId.fromString).toSet
  }

  protected def parseLabels(labelsString: String): Labels = {
    if(labelsString == null) ""
    else StringEscaper.unescape(labelsString)//StringEscaper.split(labelsString, GradoopConstants.LABEL_DELIMITER)
      //.map(StringEscaper.unescape)
  }

  protected def parseProperties(propertiesString: String): Properties = {
    if(propertiesString == null) Map[String, PV]("all" -> PropertyValue(""))
    else Map[String, PV]("all" -> PropertyValue(propertiesString))
  } // TODO
}
