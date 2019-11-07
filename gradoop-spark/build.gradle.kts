/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Scala library project to get you started.
 * For more details take a look at the Scala plugin chapter in the Gradle
 * User Manual available at https://docs.gradle.org/5.6.3/userguide/scala_plugin.html
 */

description = "Gradoop Spark"

dependencies {
    implementation(project(":gradoop-spark-common"))
    testImplementation(project(path = ":gradoop-spark-common", configuration = "testArtifact"))

    implementation("org.scala-lang:scala-reflect:${Versions.scalaFull}")

    implementation("org.apache.spark:spark-core_${Versions.scalaMajor}:${Versions.spark}")
    implementation("org.apache.spark:spark-sql_${Versions.scalaMajor}:${Versions.spark}")

    implementation("com.google.guava:guava:${Versions.guava}")
}