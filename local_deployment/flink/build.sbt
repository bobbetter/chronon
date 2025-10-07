ThisBuild / scalaVersion := "2.12.19"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "ai.chronon"

lazy val flinkVersion = "1.18.1"

lazy val root = (project in file(".")).
  settings(
    name := "simple-kafka-aggregation",
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % Provided,
      "org.apache.flink" % "flink-clients" % flinkVersion % Provided,
      "org.apache.flink" % "flink-connector-kafka" % "3.0.2-1.18",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0"
    ),
    Compile / scalaSource := baseDirectory.value / "src" / "main" / "scala",
    Test / scalaSource := baseDirectory.value / "src" / "test" / "scala"
  )

// assembly settings
import sbtassembly.AssemblyPlugin
import sbtassembly.AssemblyPlugin.autoImport._
enablePlugins(AssemblyPlugin)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("module-info.class")  => MergeStrategy.discard
  case x =>
    val old = (assembly / assemblyMergeStrategy).value
    old(x)
}

assembly / test := {}

