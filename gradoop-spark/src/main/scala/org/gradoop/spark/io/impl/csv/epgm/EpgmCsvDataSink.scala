package org.gradoop.spark.io.impl.csv.epgm

import org.gradoop.spark.io.impl.csv.CsvConstants.ComposeFunction
import org.gradoop.spark.io.impl.csv.{CsvDataSink, MetaData}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}

class EpgmCsvDataSink(csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC], metadata: Option[MetaData])
  extends CsvDataSink[G, V, E, LG, GC](csvPath, config, metadata) {

  override def graphHeadComposeFunctions: Array[ComposeFunction[EpgmGraphHead]] = {
    Array[ComposeFunction[G]](composeId, composeLabels, composeProperties)
  }

  override def vertexComposeFunctions: Array[ComposeFunction[EpgmVertex]] = {
    Array[ComposeFunction[V]](composeId, composeGraphIds, composeLabels, composeProperties)
  }

  override def edgeComposeFunctions: Array[ComposeFunction[EpgmEdge]] = {
    Array[ComposeFunction[E]](composeId, composeGraphIds, composeSourceId, composeTargetId, composeLabels, composeProperties)
  }
}

object EpgmCsvDataSink {

  def apply(csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC]): EpgmCsvDataSink = {
    new EpgmCsvDataSink(csvPath, config, None)
  }

  def apply(csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC], metadata: MetaData): EpgmCsvDataSink = {
    new EpgmCsvDataSink(csvPath, config, Some(metadata))
  }
}