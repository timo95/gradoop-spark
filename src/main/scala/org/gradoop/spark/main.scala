package org.gradoop.spark

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.model.impl.layouts.GVELayout

object main {
    def main(args: Array[String]): Unit = {

        val path = "C:/Users/timo/Desktop/Projekte/shakespeare.txt" // Should be some file on your system
        val spark = SparkSession.builder
          .appName("Simple Application")
          .master("local")
          .getOrCreate()


        val sc = spark.sparkContext
        val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
        import sqlcontext.implicits._


        val data = Seq(("Person", 13),("Tier", 3),("Person", 42))
        val dataRDD = sc.parallelize(data)


        val vertices = dataRDD.toDF("label", "age")

        val graph = new GVELayout

        spark.stop()
    }
}