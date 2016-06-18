bin/kamanja get all models
 
bin/kamanja remove model com.ligadata.samples.models.helloworldjythonmodel.00000000000000000
  
bin/kamanja add model scala `pwd`/input/SampleApplications/metadata/model/HelloWorldJython.scala DEPENDSON helloworldjythonmodel TENANTID tenant1 

input/SampleApplications/bin/PushSampleDataToKafka_HelloWorld.sh

vi config/log4j2.xml 

bin/StartEngine.sh debug
