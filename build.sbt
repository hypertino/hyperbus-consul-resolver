crossScalaVersions := Seq("2.12.4", "2.11.12")

scalaVersion in Global := crossScalaVersions.value.head

organization := "com.hypertino"

name := "hyperbus-consul-resolver"

version := "0.3-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.hypertino" %% "hyperbus" % "0.5-SNAPSHOT",
  "com.hypertino" %% "typesafe-config-binders" % "0.2.0",
  "com.orbitz.consul" % "consul-client" % "0.17.0",
  "com.google.guava" % "guava" % "22.0",
  "org.scalamock"   %% "scalamock-scalatest-support" % "3.5.0" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.8" % "test",
  compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)

fork in Test := true
