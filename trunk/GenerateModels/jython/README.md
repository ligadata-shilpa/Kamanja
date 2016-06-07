Install jython

````sbt
project MetadataAPI
assembly
project jtm
assembly
````

````bash
export enlistment=~/Kamanja/trunk
export CLASSPATH=$(find $enlistment/lib_managed/jars -name '*.jar' -printf '%p:' | sed 's/:$//')
export CLASSPATH=$CLASSPATH:$enlistment/MetadataAPI/target/scala-2.10/MetadataAPI_2.10-1.4.1.jar:$enlistment/GenerateModels/jython/target/scala-2.10/jython-assembly-1.0.jar:~/.sbt/boot/scala-2.10.5/lib/scala-reflect.jar

~/bin/jython/bin/jython ~/Kamanja/trunk/GenerateModels/jython/HelloWorld.jython 

~/Kamanja/trunk/SampleApplication/EasyInstall/easyInstallKamanja.sh  ~/app2 ~/Kamanja/trunk ~/.ivy2/ ~/sandbox/kafka_2.10-0.8.2.2/

````
