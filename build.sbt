val sparkVersion = settingKey[String]("Spark version")

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    inThisBuild(
      List(
        organization := "io.weaviate",
        scalaVersion := "2.12.18"
      )
    ),
    Defaults.itSettings,
    name := "confluent-connector",
    version := "0.0.1",
    sparkVersion := "3.4.0",
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
      "io.weaviate" %% "spark-connector" % "1.2.8",
      "org.scalatest" %% "scalatest" % "3.2.2" % "test,it",
      "org.scalacheck" %% "scalacheck" % "1.15.2" % "test",
      "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion.value}_1.4.3" % "test,it",
      "com.dimafeng" %% "testcontainers-scala" % "0.40.17" % "it"
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
    pomIncludeRepository := { x => false },
    resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),
    pomIncludeRepository := { _ => false },

    // publish settings
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    }
  )
