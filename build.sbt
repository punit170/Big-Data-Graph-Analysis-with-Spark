ThisBuild / version := "0.1.0-SNAPSHOT"

Global / excludeLintKeys += test / fork
Global / excludeLintKeys += run / mainClass

val scalaTestVersion = "3.2.11"
val typeSafeConfigVersion = "1.4.2"
val logbackVersion = "1.2.10"
//val sfl4sVersion = "2.0.0-alpha5"
val netBuddyVersion = "1.14.4"
val sparkVersion = "3.4.1"

lazy val commonDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "com.typesafe" % "config" % typeSafeConfigVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "net.bytebuddy" % "byte-buddy" % netBuddyVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
  "org.yaml" % "snakeyaml" % "1.28",
).map(_.exclude("org.slf4j", "*"))


lazy val root = (project in file("."))
  .settings(
    scalaVersion := "2.12.15",
    name := "MitMStatSim",
    libraryDependencies ++= commonDependencies ++ Seq(
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
      "org.apache.spark" %% "spark-core" % "3.4.1" /*% "provided"*/),

    libraryDependencies  ++= Seq("ch.qos.logback" % "logback-classic" % logbackVersion),
).dependsOn(NetGraph, Utilities)

lazy val NetGraph = (project in file("NetGraph"))
  .settings(
    scalaVersion := "2.12.15",
    name := "NetGraph",
    libraryDependencies ++= commonDependencies,
    libraryDependencies  ++= Seq("ch.qos.logback" % "logback-classic" % logbackVersion)
  )

lazy val Utilities = (project in file("Utilities"))
  .settings(
    scalaVersion := "2.12.15",
    name := "Utilities",
    libraryDependencies ++= commonDependencies,
    libraryDependencies  ++= Seq("ch.qos.logback" % "logback-classic" % logbackVersion)
  )

scalacOptions ++= Seq(
  "-deprecation", // emit warning and location for usages of deprecated APIs
  "-feature" // emit warning and location for usages of features that should be imported explicitly
)

compileOrder := CompileOrder.JavaThenScala
test / fork := true
run / fork := true

Compile / unmanagedSourceDirectories += baseDirectory.value / "NetGraph/src/main/scala/NetGraphAlgebraDefs"
//Runtime / unmanagedSourceDirectories += baseDirectory.value / "src/main/NetGraphAlgebraDefs"

assembly/assemblyJarName := "MitmStatSim.jar"

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}



