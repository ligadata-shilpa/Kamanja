name := "DockerManager"

version := "1.0"

shellPrompt := { state =>  "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

//libraryDependencies += "com.spotify" % "docker-client" % "2.7.7"

libraryDependencies += "com.github.docker-java" % "docker-java" % "2.2.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"