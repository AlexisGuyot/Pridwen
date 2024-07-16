scalaVersion := "2.13.8"

name := "eval_pridwen"
organization := "fr.u-bourgogne.lib.sd"
version := "1.0"
fork := true

// Shapeless
resolvers ++= Resolver.sonatypeOssRepos("releases")
resolvers ++= Resolver.sonatypeOssRepos("snapshots")
libraryDependencies += "com.chuusai" %% "shapeless" % "2.3.3"

// UPickles + OS-lib
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.9.1"
libraryDependencies += "com.lihaoyi" %% "upickle" % "3.1.3"

// Parallel collections
libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"

// Scala Reflect
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

// Breeze
libraryDependencies  ++= Seq(
  "org.scalanlp" %% "breeze" % "1.1",
  "org.scalanlp" %% "breeze-natives" % "1.1",
  "org.scalanlp" %% "breeze-viz" % "1.1"
)

// Scala XML
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "2.2.0"

// Spark
val sparkVersion = "3.5.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-client-api" % "3.3.4"
)

// Gephi
resolvers ++= Seq(
  "gephi-thirdparty" at "https://raw.github.com/gephi/gephi/mvn-thirdparty-repo/"
)
libraryDependencies += "org.gephi" % "gephi-toolkit" % "0.10.1" classifier "all"