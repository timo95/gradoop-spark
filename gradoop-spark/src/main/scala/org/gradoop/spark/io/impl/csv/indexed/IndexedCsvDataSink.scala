package org.gradoop.spark.io.impl.csv.indexed

import java.io.IOException

import org.apache.spark.sql.SaveMode
import org.gradoop.spark.io.api.DataSink
import org.gradoop.spark.io.impl.csv.CsvConstants._
import org.gradoop.spark.io.impl.csv.{CsvDataSinkBase, CsvMetaDataSink}
import org.gradoop.spark.io.impl.metadata.MetaData
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.StringEscaper

class IndexedCsvDataSink[L <: Tfl[L]] private (csvPath: String, config: GradoopSparkConfig[L],
  metaDataOpt: Option[MetaData]) extends CsvDataSinkBase(csvPath) with DataSink[L] {
  import config.Implicits._

  override def write(logicalGraph: L#LG, saveMode: SaveMode): Unit = {
    if(handleSavemode(saveMode)) {
      val metaData = metaDataOpt.getOrElse(MetaData(logicalGraph))
      logicalGraph.graphHeadsWithProperties.foreach(t => writeGraphHeads(indexedCsvPath(GRAPH_HEAD_PATH, t._1), t._2,
        metaData.graphHeadMetaData(t._1), SaveMode.Append)) // Different labels can have the same directory -> Append
      logicalGraph.verticesWithProperties.foreach(t => writeVertices(indexedCsvPath(VERTEX_PATH, t._1), t._2,
        metaData.vertexMetaData(t._1), SaveMode.Append))
      logicalGraph.edgesWithProperties.foreach(t => writeEdges(indexedCsvPath(EDGE_PATH, t._1), t._2,
        metaData.edgeMetaData(t._1), SaveMode.Append))
      CsvMetaDataSink(csvPath).write(metaData, saveMode)
    }
  }

  override def write(graphCollection: L#GC, saveMode: SaveMode): Unit = {
    if(handleSavemode(saveMode)) {
      val metaData = metaDataOpt.getOrElse(MetaData(graphCollection))
      graphCollection.graphHeadsWithProperties.foreach(t => writeGraphHeads(indexedCsvPath(GRAPH_HEAD_PATH, t._1), t._2,
        metaData.graphHeadMetaData(t._1), SaveMode.Append))
      graphCollection.verticesWithProperties.foreach(t => writeVertices(indexedCsvPath(VERTEX_PATH, t._1), t._2,
        metaData.vertexMetaData(t._1), SaveMode.Append))
      graphCollection.edgesWithProperties.foreach(t => writeEdges(indexedCsvPath(EDGE_PATH, t._1), t._2,
        metaData.edgeMetaData(t._1), SaveMode.Append))
      CsvMetaDataSink(csvPath).write(metaData, saveMode)
    }
  }

  private def handleSavemode(saveMode: SaveMode): Boolean = {
    import org.apache.hadoop.fs.{FileSystem, Path}

    val dirs = Seq(GRAPH_HEAD_PATH, VERTEX_PATH, EDGE_PATH)
    val fs = FileSystem.get(config.sparkSession.sparkContext.hadoopConfiguration)
    val exists = fs.listStatus(new Path(csvPath))
      .filter(_.isDirectory)
      .map(_.getPath.getName)
      .exists(dirs.contains)

    (exists, saveMode) match {
      case (true, SaveMode.Overwrite) => deleteCsv(); true // delete and write graph
      case (true, SaveMode.ErrorIfExists) => throw new IOException("Csv Graph already exists in: " + csvPath)
      case (true, SaveMode.Ignore) => false // don't write graph
      case _ => true // write graph
    }
  }

  private def deleteCsv(): Unit = {
    import org.apache.hadoop.fs.{FileSystem, Path}
    val fs = FileSystem.get(config.sparkSession.sparkContext.hadoopConfiguration)
    fs.delete(new Path(csvPath + DIRECTORY_SEPARATOR + GRAPH_HEAD_PATH), true)
    fs.delete(new Path(csvPath + DIRECTORY_SEPARATOR + VERTEX_PATH), true)
    fs.delete(new Path(csvPath + DIRECTORY_SEPARATOR + EDGE_PATH), true)
  }

  private def indexedCsvPath(elementPath: String, label: String): String = {
    val dir = if (label.isEmpty) DEFAULT_DIRECTORY.toString
    else cleanFilename(StringEscaper.escape(label, ESCAPED_CHARS))
    elementPath + DIRECTORY_SEPARATOR + dir + DIRECTORY_SEPARATOR + SIMPLE_FILE
  }

  private def cleanFilename(filename: String): String = filename.replaceAll("[<>:\"/\\\\|?*]", "_").toLowerCase
}

object IndexedCsvDataSink {

  def apply[L <: Tfl[L]](csvPath: String, config: GradoopSparkConfig[L]): IndexedCsvDataSink[L] = {
    new IndexedCsvDataSink(csvPath, config, None)
  }

  def apply[L <: Tfl[L]](csvPath: String, config: GradoopSparkConfig[L], metadata: MetaData): IndexedCsvDataSink[L] = {
    new IndexedCsvDataSink(csvPath, config, Some(metadata))
  }
}