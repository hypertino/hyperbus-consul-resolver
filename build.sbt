crossScalaVersions := Seq("2.12.1", "2.11.8")

scalaVersion in Global := "2.12.1"

organization := "com.hypertino"

name := "hyperbus-consul-resolver"

version := "0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.hypertino"   %% "hyperbus-transport" % "0.2-SNAPSHOT",
  "com.hypertino"   %% "hyperbus-model" % "0.2-SNAPSHOT",
  "com.orbitz.consul" % "consul-client" % "0.14.0",
  "org.scalamock"   %% "scalamock-scalatest-support" % "3.5.0" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.8" % "test",
  compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)
