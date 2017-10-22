crossScalaVersions := Seq("2.12.3", "2.11.11")

scalaVersion in Global := crossScalaVersions.value.head

organization := "com.hypertino"

name := "hyperbus-consul-resolver"

version := "0.3-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.hypertino" %% "hyperbus" % "0.3-SNAPSHOT",
  "com.hypertino" %% "typesafe-config-binders" % "0.2.0",
  "com.orbitz.consul" % "consul-client" % "0.16.5",
  "com.google.guava" % "guava" % "19.0",
  "org.scalamock"   %% "scalamock-scalatest-support" % "3.5.0" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.8" % "test",
  compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)

fork in Test := true
