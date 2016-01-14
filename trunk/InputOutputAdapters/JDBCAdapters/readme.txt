Readme.txt

Add the following to the build.sbt of the parent project to create a JAR

lazy val JDBCAdapters = project.in(file("InputOutputAdapters/JDBCAdapters")) dependsOn(InputOutputAdapterBase, Exceptions, DataDelimiters)