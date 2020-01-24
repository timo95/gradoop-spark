package org.gradoop.spark.model.impl.operators.equality

import org.gradoop.spark.model.impl.operators.equality.gve.GveEquals
import org.gradoop.spark.model.impl.operators.tostring.gve.ElementToString
import org.gradoop.spark.util.SparkAsciiGraphLoader
import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}
import org.scalatest.prop.TableDrivenPropertyChecks

class EqualityTest extends EpgmGradoopSparkTestBase with TableDrivenPropertyChecks {

  val asciiGraphs: String = "gEmpty[]" +
    "gRef:G{dataDiff : false}[" +
    "(a1:A{x : 1})-[loop:a{x : 1}]->(a1)-[aa:a{x : 1}]->(a2:A{x : 2})" + // loop around a1 and edge from a1 to a2
    "(a1)-[par1:p]->(b1:B)" + // parallel edge from a1 to b1
    "(a1)-[par2:p]->(b1:B)" +
    "(b1)-[cyc1:c]->(b2:B)-[cyc2:c]->(b3:B)-[cyc3:c]->(b1)]" + // cycle of bs
    "gClone:G{dataDiff : true}[" + // element id copy of gRef
    "(a1)-[loop]->(a1)-[aa]->(a2)" +
    "(a1)-[par1]->(b1)" +
    "(a1)-[par2]->(b1)" +
    "(b1)-[cyc1]->(b2)-[cyc2]->(b3)-[cyc3]->(b1)]" +
    "gDiffId:G{dataDiff : false}[" + // element id copy of gRef with one different edge id
    "(a1)-[loop]->(a1)-[aa]->(a2)" +
    "(a1)-[par1]->(b1)" +
    "(a1)-[par2]->(b1)" +
    "(b1)-[cyc1]->(b2)-[:c]->(b3)-[cyc3]->(b1)]" +
    "gDiffData:G[" + // with each one different vertex and edge attribute
    "(a1)-[loop]->(a1)-[:a{y : 1}]->(:A{x : \"diff\"})" +
    "(a1)-[par1]->(b1)" +
    "(a1)-[par2]->(b1)" +
    "(b1)-[cyc1]->(b2)-[cyc2]->(b3)-[cyc3]->(b1)]" +
    "gReverse:G{dataDiff : false}[" + // copy of gRef with partially reverse edges
    "(a1)-[loop]->(a1)<-[:a{x : 1}]-(a2)" +
    "(a1)<-[:p]-(b1)" +
    "(a1)-[:p]->(b1)" +
    "(b1)-[cyc1]->(b2)-[cyc2]->(b3)-[cyc3]->(b1)]"

  val loader: SparkAsciiGraphLoader[L] = SparkAsciiGraphLoader.fromString(getConfig, asciiGraphs)

  describe("Equality GraphCollection") {

    it("equals by graph ids - directed", OperatorTest) {
      val graphCollections = Table(
        ("left", "right", "equals"),
        ("gRef", "gRef", true),
        ("gRef", "gClone", false),
        ("gRef", "gEmpty", false)
      )
      forEvery(graphCollections) { (left, right, equals) =>
        val leftCollection = loader.getGraphCollectionByVariables(left)
        val rightCollection = loader.getGraphCollectionByVariables(right)
        assert(leftCollection.equalsByGraphIds(rightCollection) == equals)
      }
    }

    it("equals by graph element ids - directed", OperatorTest) {
      val graphCollections = Table(
        ("left", "right", "equals"),
        (Seq("gRef", "gClone", "gEmpty"), Seq("gClone", "gRef", "gEmpty"), true),
        (Seq("gRef", "gClone", "gEmpty"), Seq("gRef", "gDiffId", "gEmpty"), false),
        (Seq("gRef", "gClone", "gEmpty"), Seq("gRef", "gRef"), false),
        (Seq("gRef", "gClone", "gEmpty"), Seq.empty[String], false)
      )
      forEvery(graphCollections) { (left, right, equals) =>
        val leftCollection = loader.getGraphCollectionByVariables(left: _*)
        val rightCollection = loader.getGraphCollectionByVariables(right: _*)
        assert(leftCollection.equalsByGraphElementIds(rightCollection) == equals)
      }
    }

    it("equals by graph element data - directed", OperatorTest) {
      val graphCollections = Table(
        ("left", "right", "equals"),
        (Seq("gRef", "gClone", "gEmpty"), Seq("gClone", "gRef", "gEmpty"), true),
        (Seq("gRef", "gClone", "gEmpty"), Seq("gRef", "gDiffData", "gEmpty"), false),
        (Seq("gRef", "gClone", "gEmpty"), Seq("gRef", "gRef"), false),
        (Seq("gRef", "gClone", "gEmpty"), Seq.empty[String], false)
      )
      forEvery(graphCollections) { (left, right, equals) =>
        val leftCollection = loader.getGraphCollectionByVariables(left: _*)
        val rightCollection = loader.getGraphCollectionByVariables(right: _*)
        assert(leftCollection.equalsByGraphElementData(rightCollection) == equals)
      }
    }

    it("equals by graph data - directed", OperatorTest) {
      val graphCollections = Table(
        ("left", "right", "equals"),
        (Seq("gRef", "gEmpty"), Seq("gDiffId", "gEmpty"), true),
        (Seq("gRef", "gEmpty"), Seq("gClone", "gEmpty"), false),
        (Seq("gRef", "gEmpty"), Seq("gDiffData", "gEmpty"), false),
        (Seq("gRef", "gEmpty"), Seq("gRef"), false),
        (Seq("gRef", "gEmpty"), Seq.empty[String], false)
      )
      forEvery(graphCollections) { (left, right, equals) =>
        val leftCollection = loader.getGraphCollectionByVariables(left: _*)
        val rightCollection = loader.getGraphCollectionByVariables(right: _*)
        assert(leftCollection.equalsByGraphData(rightCollection) == equals)
      }
    }
  }

  // graphs to compare and equality for different modes
  private val logicalGraphs = Table(
    ("left", "right", "by element id", "by element data", "by data", "by undirected data"),
    ("gRef", "gEmpty", false, false, false, false),
    ("gRef", "gClone", true, true, false, false),
    ("gRef", "gDiffId", false, true, true, true),
    ("gRef", "gDiffData", false, false, false, false),
    ("gRef", "gReverse", false, false, false, true)
  )

  describe("Equality LogicalGraph") {
    it("equals by element id - directed", OperatorTest) {
      forEvery(logicalGraphs) { (left, right, equal, _, _, _) =>
        val leftGraph = loader.getLogicalGraphByVariable(left)
        val rightGraph = loader.getLogicalGraphByVariable(right)
        assert(leftGraph.equalsByElementIds(rightGraph) == equal)
      }
    }
    it("equals by element data - directed", OperatorTest) {
      forEvery(logicalGraphs) { (left, right, _, equal, _, _) =>
        val leftGraph = loader.getLogicalGraphByVariable(left)
        val rightGraph = loader.getLogicalGraphByVariable(right)
        assert(leftGraph.equalsByElementData(rightGraph) == equal)
      }
    }
    it("equals by data - directed", OperatorTest) {
      forEvery(logicalGraphs) { (left, right, _, _, equal, _) =>
        val leftGraph = loader.getLogicalGraphByVariable(left)
        val rightGraph = loader.getLogicalGraphByVariable(right)
        assert(leftGraph.equalsByData(rightGraph) == equal)
      }
    }
    it("equals by data - undirected", OperatorTest) {
      forEvery(logicalGraphs) { (left, right, _, _, _, equal) =>
        val leftGraph = loader.getLogicalGraphByVariable(left)
        val rightGraph = loader.getLogicalGraphByVariable(right)
        val operator = new GveEquals[L](ElementToString.graphHeadToDataString, ElementToString.vertexToDataString,
          ElementToString.edgeToDataString, false)
        assert(leftGraph.callForValue(operator, rightGraph) == equal)
      }
    }
  }
}
