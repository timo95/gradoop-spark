

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
    apply(plugin = "java-library")
    apply(plugin = "scala")

    // Compatible class version needed for cluster
    configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    // Runs scalatest tests
    apply(plugin = "com.github.maiflai.scalatest")

    tasks.getByName<Test>("test") {
        maxParallelForks = 1

        minHeapSize = "512m"
        maxHeapSize = "2g"

        extensions.get("tags").delegateClosureOf<PatternSet> {
        //"tags".delegateClosureOf<PatternSet> {
            // TODO: Does not do anything yet, should exclude operator tests (#9)
            exclude("org.scalatest.tags.operator")
        }
    }

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
        // Scala
        "implementation"("org.scala-lang:scala-library:${Versions.scalaFull}")

        // Testing framework
        "testImplementation"("org.scalatest:scalatest_${Versions.scalaMajor}:${Versions.scalatest}")

        // Needed by scalatest plugin "com.github.maiflai.scalatest"
        "testRuntimeOnly"("org.pegdown:pegdown:${Versions.pegdown}")
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
