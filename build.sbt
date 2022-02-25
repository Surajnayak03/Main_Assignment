import sbt._
import Keys._
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "M0vie_data_spark"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"

