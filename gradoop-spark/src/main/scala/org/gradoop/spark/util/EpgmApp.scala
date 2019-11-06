package org.gradoop.spark.util

import org.apache.spark.sql.SparkSession
import org.gradoop.common.model.api.types.ComponentTypes
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollectionFactory, LogicalGraphFactory}
import org.gradoop.spark.model.impl.layouts.EpgmGveLayout
import org.gradoop.spark.model.impl.types.EpgmTypes

trait EpgmApp extends EpgmTypes with ComponentTypes {

  private var config: GradoopSparkConfig[G, V, E, LG, GC] = _

  def getGveConfig(implicit session: SparkSession): GradoopSparkConfig[G, V, E, LG, GC] = {
    if (config == null) {
      config = new GradoopSparkConfig[G, V, E, LG, GC](null, null)

      config.setLogicalGraphFactory(new LogicalGraphFactory[G, V, E, LG, GC](EpgmGveLayout, config))
      config.setGraphCollectionFactory(new GraphCollectionFactory[G, V, E, LG, GC](EpgmGveLayout, config))
    }
    config
  }

  def getGraphGDLString: String = {
    "db[" +
      "(databases:tag {name:\"Databases\"})" +
      "(graphs:Tag {name:\"Graphs\"})" +
      "(hadoop:Tag {name:\"Hadoop\"})" +
      "(gdbs:Forum {title:\"Graph Databases\"})" +
      "(gps:Forum {title:\"Graph Processing\"})" +
      "(alice:Person {name:\"Alice\", gender:\"f\", city:\"Leipzig\", birthday:20})" +
      "(bob:Person {name:\"Bob\", gender:\"m\", city:\"Leipzig\", birthday:30})" +
      "(carol:Person {name:\"Carol\", gender:\"f\", city:\"Dresden\", birthday:30})" +
      "(dave:Person {name:\"Dave\", gender:\"m\", city:\"Dresden\", birthday:40})" +
      "(eve:Person {name:\"Eve\", gender:\"f\", city:\"Dresden\", speaks:\"English\", birthday:35})" +
      "(frank:Person {name:\"Frank\", gender:\"m\", city:\"Berlin\", locIP:\"127.0.0.1\", birthday:35})" +
      "" +
      "(eve)-[:hasInterest]->(databases)" +
      "(alice)-[:hasInterest]->(databases)" +
      "(frank)-[:hasInterest]->(hadoop)" +
      "(dave)-[:hasInterest]->(hadoop)" +
      "(gdbs)-[:hasModerator]->(alice)" +
      "(gdbs)-[:hasMember]->(alice)" +
      "(gdbs)-[:hasMember]->(bob)" +
      "(gps)-[:hasModerator {since:2013}]->(dave)" +
      "(gps)-[:hasMember]->(dave)" +
      "(gps)-[:hasMember]->(carol)-[ckd]->(dave)" +
      "(databases)<-[ghtd:hasTag]-(gdbs)-[ghtg1:hasTag]->(graphs)<-[ghtg2:hasTag]-(gps)-[ghth:hasTag]->(hadoop)" +
      "(eve)-[eka:knows {since:2013}]->(alice)-[akb:knows {since:2014}]->(bob)" +
      "(eve)-[ekb:knows {since:2015}]->(bob)-[bka:knows {since:2014}]->(alice)" +
      "(frank)-[fkc:knows {since:2015}]->(carol)-[ckd:knows {since:2014}]->(dave)" +
      "(frank)-[fkd:knows {since:2015}]->(dave)-[dkc:knows {since:2014}]->(carol)" +
      "(alice)-[akb]->(bob)-[bkc:knows {since:2013}]->(carol)-[ckd]->(dave)" +
      "(alice)<-[bka]-(bob)<-[ckb:knows {since:2013}]-(carol)<-[dkc]-(dave)" +
      "]";
  }
}
