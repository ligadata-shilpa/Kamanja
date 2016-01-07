name := "JpmmlFactoryOfModelInstanceFactory"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "org.jpmml" % "pmml-evaluator" % "1.2.4"

libraryDependencies += "org.jpmml" % "pmml-model" % "1.2.5"

libraryDependencies += "org.jpmml" % "pmml-schema" % "1.2.5"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.4.1"

scalacOptions += "-deprecation"
