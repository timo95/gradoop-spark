package org.gradoop.spark.io.impl.csv.epgm

import org.gradoop.spark.io.impl.csv.CsvConstants.ParseFunction
import org.gradoop.spark.io.impl.csv.{CsvDataSource, MetaData}
import org.gradoop.spark.model.api.config.GradoopSparkConfig

class EpgmCsvDataSource(csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC], metadata: Option[MetaData])
  extends CsvDataSource[G, V, E, LG, GC](csvPath, config, metadata) {

  override def getGraphHeadParseFunctions: Array[ParseFunction[G]] = {
    Array[ParseFunction[G]](createGraphHead, parseLabels, parseProperties)
  }

  override def getVertexParseFunctions: Array[ParseFunction[V]] = {
    Array[ParseFunction[V]](createVertex, parseGraphIds, parseLabels, parseProperties)
  }

  override def getEdgeParseFunctions: Array[ParseFunction[E]] = {
    Array[ParseFunction[E]](createEdge, parseGraphIds, parseSourceId, parseTargetId, parseLabels, parseProperties)
  }
}

object EpgmCsvDataSource {

  def apply(csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC]): EpgmCsvDataSource = {
    new EpgmCsvDataSource(csvPath, config, None)
  }

  def apply(csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC], metaData: MetaData): EpgmCsvDataSource = {
    new EpgmCsvDataSource(csvPath, config, Some(metaData))
  }
}
