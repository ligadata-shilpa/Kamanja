name := "DockerManager"

version := "1.0"

shellPrompt := { state =>  "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.5",
  "org.apache.logging.log4j" % "log4j-core" % "2.5",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.github.docker-java" % "docker-java" % "2.2.1"
)

coverageEnabled := false