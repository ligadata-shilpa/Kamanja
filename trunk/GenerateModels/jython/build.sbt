name := "jython"

version := "1.0"

// scalaVersion := "2.11.7"

shellPrompt := { state =>  "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

fork in (run) := true

net.virtualvoid.sbt.graph.Plugin.graphSettings

parallelExecution in Test := false

libraryDependencies += "org.python" % "jython" % "2.7.0"
