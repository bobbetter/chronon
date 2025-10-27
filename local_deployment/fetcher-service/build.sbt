name := "fetcher-service"
version := "0.1.0"
scalaVersion := "2.12.18"

enablePlugins(JavaAppPackaging)

// Akka HTTP (server backend)
val akkaVersion = "2.6.21"
val akkaHttpVersion = "10.2.10"

// Tapir (API definitions)
val tapirVersion = "1.2.10"

// Circe (JSON)
val circeVersion = "0.14.5"

libraryDependencies ++= Seq(
  // Akka HTTP
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  
  // Tapir
  "com.softwaremill.sttp.tapir" %% "tapir-core" % tapirVersion,
  "com.softwaremill.sttp.tapir" %% "tapir-akka-http-server" % tapirVersion,
  "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % tapirVersion,
  "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle" % tapirVersion,
  "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % tapirVersion,
  "com.softwaremill.sttp.apispec" %% "openapi-circe-yaml" % "0.3.2",
  
  // Circe
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,

  // CORS support
  "ch.megard" %% "akka-http-cors" % "1.1.3",
  
  // Logging
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "ch.qos.logback" % "logback-classic" % "1.3.15",
  "ch.qos.logback" % "logback-core" % "1.3.15"
)

// Main class for native packager
Compile / mainClass := Some("ai.chronon.fetcher.FetcherServiceApp")

// Include chronon-aws-assembly.jar in the classpath
Compile / unmanagedJars += baseDirectory.value / "chronon-aws-assembly.jar"

