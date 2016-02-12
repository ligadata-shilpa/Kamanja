import sbt._
import Keys._

shellPrompt := { state =>  "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

name := "DockerManager"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.4.1"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test"

libraryDependencies += "se.marcuslonnberg" %% "scala-docker" % "0.4.0"

//resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"

//libraryDependencies += "me.lessis" %% "tugboat" % "0.2.0"