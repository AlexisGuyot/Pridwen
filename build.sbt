scalaVersion := "2.13.8"

name := "pridwen"
organization := "fr.u-bourgogne.lib.sd"
version := "1.0"
//fork := true

// Shapeless
resolvers ++= Resolver.sonatypeOssRepos("releases")
resolvers ++= Resolver.sonatypeOssRepos("snapshots")
libraryDependencies += "com.chuusai" %% "shapeless" % "2.3.3"

// Scala Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14"

// Parallel collections
libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"

// Spark
val sparkVersion = "3.5.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-client-api" % "3.3.4"
)

// Breeze
libraryDependencies  ++= Seq(
  "org.scalanlp" %% "breeze" % "1.1",
  "org.scalanlp" %% "breeze-natives" % "1.1",
  "org.scalanlp" %% "breeze-viz" % "1.1"
)