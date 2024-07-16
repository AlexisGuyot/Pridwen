scalaVersion := "2.13.8"

name := "pridwen"
organization := "fr.u-bourgogne.lib.sd"
version := "1.0"

// Shapeless
resolvers ++= Resolver.sonatypeOssRepos("releases")
resolvers ++= Resolver.sonatypeOssRepos("snapshots")
libraryDependencies += "com.chuusai" %% "shapeless" % "2.3.3"

// Scala Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14"

// Parallel collections
libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"