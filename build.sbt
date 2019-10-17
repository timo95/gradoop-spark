name := "gradoop-spark"

version := "0.1"

scalaVersion := "2.12.10"


libraryDependencies += "org.gradoop" % "gradoop-common" % "0.5.0"

libraryDependencies += "org.apache.flink" % "flink-core" % "1.7.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
