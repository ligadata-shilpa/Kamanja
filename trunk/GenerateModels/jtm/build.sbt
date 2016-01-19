name := "jtm"

version := "1.0"

scalaVersion := "2.11.7"

shellPrompt := { state =>  "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

fork in (run) := true

net.virtualvoid.sbt.graph.Plugin.graphSettings

libraryDependencies += "org.rogach" % "scallop_2.11" % "0.9.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4"

