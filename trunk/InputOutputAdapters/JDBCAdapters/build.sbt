// Project name (artifact name in Maven)
name := "JDBCAdapters"

// orgnization name (e.g., the package name of the project)
organization := "com.ligadata"

version := "1.0-SNAPSHOT"

// project description
description := "JDBC Adapters for creating data pipelines"

EclipseKeys.withSource := true

EclipseKeys.relativizeLibs := false

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.9" 

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.9"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.4.1"

libraryDependencies += "commons-codec" % "commons-codec" % "1.10"
       
libraryDependencies += "org.scala-lang" % "scala-actors" % "2.11.7"

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.1.1"