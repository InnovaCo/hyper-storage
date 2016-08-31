import sbt.Keys._

name := "hyper-storage"

organization := "eu.inn"

projectMajorVersion := "0.1"

projectBuildNumber := "SNAPSHOT"

version := projectMajorVersion.value + "." + projectBuildNumber.value

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8")

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Innova releases" at "http://repproxy.srv.inn.ru/artifactory/libs-release-local"
)

ramlHyperbusSource := file("hyperstorage.raml")

ramlHyperbusPackageName := "eu.inn.hyperstorage.api"

buildInfoPackage := "eu.inn.hyperstorage"

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)


// BuildInfo
lazy val root = (project in file(".")).enablePlugins(BuildInfoPlugin, Raml2Hyperbus)

val projectMajorVersion = settingKey[String]("Defines the major version number")

val projectBuildNumber = settingKey[String]("Defines the build number")

// Macro Paradise
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "eu.inn" %% "service-control" % "0.2.17",
  "eu.inn" %% "service-config" % "0.1.6",
  "eu.inn" %% "service-metrics" % "0.2.9",
  "eu.inn" %% "binders-typesafe-config" % "0.12.15",
  "eu.inn" %% "hyperbus" % "0.1.80",
  "eu.inn" %% "hyperbus-t-distributed-akka" % "0.1.80",
  "eu.inn" %% "hyperbus-akka" % "0.1.80",
  "eu.inn" %% "hyperbus-t-kafka" % "0.1.80",
  "eu.inn" %% "binders-core" % "0.12.93",
  "eu.inn" %% "binders-json" % "0.12.47",
  "eu.inn" %% "binders-cassandra" % "0.13.50",
  "eu.inn" %% "expression-parser" % "0.1.29",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.9",
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.1" % "test",
  "org.cassandraunit" % "cassandra-unit" % "2.2.2.1" % "test",
  "junit" % "junit" % "4.12" % "test",
  "org.pegdown" % "pegdown" % "1.6.0" % "test",
  "com.storm-enroute" %% "scalameter" % "0.7" % "test"
)

testFrameworks += new TestFramework(
  "org.scalameter.ScalaMeterFramework")

logBuffered := false

testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports", "-oDS")

parallelExecution in Test := false

