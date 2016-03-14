import sbt.Keys._

name := "revault"

organization := "eu.inn"

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.11.7")

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Innova releases" at "http://repproxy.srv.inn.ru/artifactory/libs-release-local"
)

ramlHyperBusSource := file("revault.raml")

ramlHyperBusPackageName := "eu.inn.revault.api"

buildInfoPackage := "eu.inn.revault"

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, buildInfoBuildNumber)

// BuildInfo
lazy val root = (project in file(".")). enablePlugins(BuildInfoPlugin, Raml2HyperBus)

// Macro Paradise
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "eu.inn" %% "service-control" % "0.1.16",
  "eu.inn" %% "service-config" % "0.1.3",
  "eu.inn" %% "hyperbus" % "0.1.60",
  "eu.inn" %% "hyperbus-t-distributed-akka" % "0.1.60",
  "eu.inn" %% "hyperbus-akka" % "0.1.60",
  "eu.inn" %% "binders-core" % "0.11.77",
  "eu.inn" %% "binders-cassandra" % "0.9.42",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.9",
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.1" % "test",
  "org.cassandraunit" % "cassandra-unit" % "2.2.2.1" % "test",
  "junit" % "junit" % "4.12" % "test"
)
