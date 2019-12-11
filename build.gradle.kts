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

    // Scala style
    id("com.github.alisiikh.scalastyle") version "3.1.1" apply false

    // Run scalatest tests with gradle build
    id("com.github.maiflai.scalatest") version "0.25" apply false

    // Shade equivalent
    id("com.github.johnrengelman.shadow") version "5.2.0" apply false
}

allprojects {
    group = "org.gradoop.spark"
    version = Versions.self
}

subprojects {
    //apply(plugin = "java-library")
    apply(plugin = "scala")

    // Run scalatest tests
    apply(plugin = "com.github.maiflai.scalatest")

    // Scala style
    apply(plugin = "com.github.alisiikh.scalastyle")
    configure<com.github.alisiikh.scalastyle.ScalastyleExtension> {
        setConfig(file("${rootDir}/scalastyle_config.xml"))
    }

    repositories {
        mavenLocal()
        mavenCentral()
    }

    dependencies {
        // Needed by scalatest plugin "com.github.maiflai.scalatest"
        "testRuntime"("org.pegdown:pegdown:1.4.2")

        // Scala
        "implementation"("org.scala-lang:scala-library:${Versions.scalaFull}")

        // Testing framework
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
