import sbt.Keys._

name := "revault"

organization := "eu.inn"

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.11.7")

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Innova releases" at "http://repproxy.srv.inn.ru/artifactory/libs-release-local"
)

// BuildInfo
lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, buildInfoBuildNumber),
    buildInfoPackage := "eu.inn.revault"
  )

// Macro Paradise
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "eu.inn" %% "service-control" % "0.1.16",
  "eu.inn" %% "service-config" % "0.1.3",
  "eu.inn" %% "hyperbus" % "0.1.48",
  "eu.inn" %% "hyperbus-t-distributed-akka" % "0.1.SNAPSHOT",
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test"
)
