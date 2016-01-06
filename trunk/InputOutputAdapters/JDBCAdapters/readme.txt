Readme.txt

Packages
1. com.ligadata.adapters.pipeline - Contains the main classes for building and running Pipelines. 
a. IPipeline is the main interface. 
b. PipelineRunner is the class that takes a config file, creates and runs a pipeline. Added a sample configuration tableRunnerConfig1.json (which can be used to create and run a JDBC pipeline). Invocation details - com.ligadata.adapters.pipeline.PipelineRunner -c tableRunnerConfig1.json 
c. PipulineBuilder class takes configs and creates approprpate plumbing classes, Reader, Writer, Processor. Will be extending this class to incorporate all the Readers/Writers/Processors

2. com.ligadata.adapters.processor - Processor classes, for converting data. A record processor takes in an InputRecord (Map Record), and converts it to a target format. There are currently two different formats implementations, Delimited Format and JSON Format. We can specify various flags and parameters for the Delimited Record Processor(KeyValue Separator Character, Column Separator, Row Separator).

3. com.ligadata.adapters.record - RecordReaders, for reading data from source. Current implementations include TableReader and QueryReader, which read from a DB Table or a specific DB query

4. com.ligadata.adapters.utility - Utility classes 

5. com.ligadata.adapters.writer - Classes to writeout a Record to a persistent store. Implementations include a FileWriter (for Demimited Records), and a JSONFileWriter (for JSON Records) and a simple KafkaWriter, using the new Producer API.

6. com.ligadata.adapters.test - Contains a test class to insert records at fixed intervals into a DB table. Table creation is also handled here (if it doesnot exist). 
a. PopulateDBData - Class which does the insertion. Invocation Details - com.ligadata.adapters.test.PopulateDBData -p testDBProperties.json