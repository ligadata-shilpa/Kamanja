name := "SmartFileAdapter"

version := "1.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Apache repo" at "https://repository.apache.org/content/repositories/releases"

libraryDependencies += "org.apache.logging.log4j" % "log4j-1.2-api" % "2.4.1"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.4.1"

libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-actors" % _)

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.9"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.9"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1"

libraryDependencies += "org.apache.commons" % "commons-vfs2" % "2.0"

libraryDependencies += "org.apache.commons" % "commons-compress" % "1.5"

libraryDependencies += "org.anarres.lzo" % "lzo-core" % "1.0.0"