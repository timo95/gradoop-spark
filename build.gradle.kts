/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Scala library project to get you started.
 * For more details take a look at the Scala plugin chapter in the Gradle
 * User Manual available at https://docs.gradle.org/5.6.3/userguide/scala_plugin.html
 */

plugins {
    base

    // Fix for gradle + Windows bug
    id("com.github.ManifestClasspath") version "0.1.0-RELEASE"
}

allprojects {
    group = "org.gradoop.spark"
    version = Versions.self
}

subprojects {
    apply(plugin = "scala")

    repositories {
        mavenLocal()
        mavenCentral()
    }

    dependencies {
        "implementation"("org.scala-lang:scala-library:${Versions.scalaFull}")

        "testImplementation"("org.scalatest:scalatest_${Versions.scalaMajor}:${Versions.scalatest}")
    }

    // make tests available in a test jar
    val testJar by tasks.registering(Jar::class) {
        archiveClassifier.set("tests")
        from(project.the<SourceSetContainer>()["test"].output)
    }
    val testArtifact by configurations.creating

    artifacts {
        add(testArtifact.name, testJar)
    }
}

dependencies {
    subprojects.forEach {
        archives(it)
    }
}
