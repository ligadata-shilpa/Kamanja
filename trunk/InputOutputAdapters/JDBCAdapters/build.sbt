// Project name (artifact name in Maven)
name := "JDBCAdapters"

// orgnization name (e.g., the package name of the project)
organization := "com.ligadata"

version := "1.0-SNAPSHOT"

// project description
description := "JDBC Adapters for creating data pipelines"

// Enables publishing to maven repo
publishMavenStyle := true

// Do not append Scala versions to the generated artifacts
crossPaths := false

// This forbids including Scala related libraries into the dependency
autoScalaLibrary := false

// library dependencies. (orginization name) % (project name) % (version)
libraryDependencies ++= Seq(
   "org.mariadb.jdbc" % "mariadb-java-client" % "1.3.2",
   "org.apache.commons" % "commons-lang3" % "3.4",
   "org.apache.commons" % "commons-dbcp2" % "2.1.1",
   "ch.qos.logback" % "logback-classic" % "1.1.3",
   "org.apache.kafka" % "kafka-clients" % "0.8.2.2",
   "org.apache.zookeeper" % "zookeeper" % "3.4.6",
   "org.projectlombok" % "lombok" % "1.16.6",
   "com.google.guava" % "guava" % "19.0",
   "joda-time" % "joda-time" % "2.9.1",
   "com.google.code.gson" % "gson" % "2.5",
   "org.easybatch" % "easybatch-core" % "4.0.0",
   "org.easybatch" % "easybatch-jdbc" % "4.0.0",
   "org.mockito" % "mockito-core" % "1.9.5" % "test"  // Test-only dependency
)
