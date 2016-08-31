import sbt.Keys._

name := "hyper-storage-perftest"

organization := "eu.inn"

version := "0.1.SNAPSHOT"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8")

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Innova releases" at "http://repproxy.srv.inn.ru/artifactory/libs-release-local"
)

ramlHyperbusSource := file("../src/main/resources/hyperstorage.raml")

ramlHyperbusPackageName := "eu.inn.hyperstorage.api"

ramlHyperbusSourceIsResource := false

// BuildInfo
lazy val root = (project in file(".")).enablePlugins(Raml2Hyperbus)

// Macro Paradise
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "eu.inn" %% "hyperbus" % "0.1.80",
  "eu.inn" %% "hyperbus-t-distributed-akka" % "0.1.80",
  "eu.inn" %% "hyperbus-akka" % "0.1.80",
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "com.storm-enroute" %% "scalameter" % "0.7"
)
