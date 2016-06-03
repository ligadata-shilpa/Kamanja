Install jython

````sbt
project MetadataAPI
assembly
project jtm
assembly
````

````bash
export CLASSPATH=/home/joerg/Kamanja/trunk/MetadataAPI/target/scala-2.10/MetadataAPI_2.10-1.4.1.jar:/home/joerg/Kamanja/trunk/GenerateModels/jtm/target/scala-2.10/jtm-assembly-1.0:.jar:/home/joerg/Kamanja/trunk/GenerateModels/testmsg/target/scala-2.10/testmsg-assembly-1.0.jar:$CLASSPATH

~/bin/jython/bin/jython ~/Kamanja/trunk/GenerateModels/jython/HelloWorld.jython 
````
