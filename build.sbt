import sbt.url
import ReleaseTransformations.*

val sparkVersion = settingKey[String]("Spark version")

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    inThisBuild(
      List(
        organization := "io.weaviate",
        organizationName := "Weaviate B.V.",
        organizationHomepage := Some(url("https://weaviate.io")),
        scmInfo := Some(
          ScmInfo(
            url("https://github.com/weaviate/confluent-connector/tree/main"),
            "scm:git@github.com:weaviate/confluent-connector.git"
          )
        ),
        developers := List(
          Developer(
            id = "hsm207",
            name = "Mohd Shukri Hasan",
            email = "shukri@weaviate.io",
            url = url("https://github.com/hsm207")
          ),
        ),
        description := "Weaviate Confluent Cloud Connector to use in Spark ETLs to populate a Weaviate vector database.",
        licenses := List(
          "Weaviate B.V. License" -> new URL("https://github.com/weaviate/confluent-connector/blob/main/LICENSE")
        ),
        homepage := Some(url("https://github.com/weaviate/confluent-connector")),
        scalaVersion := "2.12.18",
        crossScalaVersions := Seq("2.12.18", "2.13.12"),
      )
    ),
    Defaults.itSettings,
    name := "confluent-connector",
    assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar",
    sparkVersion := "3.5.0",
    javacOptions ++= Seq("-source", "1.11", "-target", "1.11"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,
    coverageHighlighting := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-avro" % sparkVersion.value % "provided",
      "io.weaviate" %% "spark-connector" % "1.2.9",
      "com.typesafe.play" %% "play-json" % "2.10.1",
      "org.scalatest" %% "scalatest" % "3.2.17" % "test,it",
      "org.scalacheck" %% "scalacheck" % "1.17.0" % "test",
      "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion.value}_1.4.4" % "test,it",
      "com.dimafeng" %% "testcontainers-scala" % "0.41.0" % "it"
    ),

    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    run in Compile := Defaults
      .runTask(
        fullClasspath in Compile,
        mainClass in (Compile, run),
        runner in (Compile, run)
      )
      .evaluated,
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _                        => MergeStrategy.first
    },
    // release settings
    pomIncludeRepository := { _ => false },
    publishMavenStyle := true,
    publishTo := sonatypePublishToBundle.value,
    sonatypeCredentialHost := "s01.oss.sonatype.org",
    sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
    sonatypeProfileName := "io.weaviate",
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      //  publishArtifacts,   // done by CI on tag push
      setNextVersion,
      commitNextVersion,
      //  pushChanges         // done manually
    ),
  )
