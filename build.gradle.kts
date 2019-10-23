import Versions.scalaFull
import Versions.scalaMajor
import Versions.scalatest

/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Scala library project to get you started.
 * For more details take a look at the Scala plugin chapter in the Gradle
 * User Manual available at https://docs.gradle.org/5.6.3/userguide/scala_plugin.html
 */

plugins {
    id("com.github.ManifestClasspath") version "0.1.0-RELEASE"
}

allprojects {
    group = "org.gradoop.spark"
    version = "0.1"//Versions.self
}

subprojects {
    apply(plugin = "scala")

    repositories {
        mavenLocal()
        mavenCentral()
    }

    dependencies {
        // Use Scala 2.12 in our library project
        "implementation"("org.scala-lang:scala-library:$scalaFull")

        // Use Scalatest for testing our library
        "testImplementation"("junit:junit:4.12")
        "testImplementation"("org.scalatest:scalatest_$scalaMajor:$scalatest")

        // Need scala-xml at test runtime
        "testRuntimeOnly"("org.scala-lang.modules:scala-xml_$scalaMajor:1.2.0")
    }
}
