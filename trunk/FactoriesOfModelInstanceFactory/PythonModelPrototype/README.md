**Scala client and Python Server**

_Status_

The code you find in the checkin is "feature complete" but not fully tested.  I have unit tested different aspects as I tried to discover what I would need for this effort in terms of the Python language.  

I will have another checkin either tomorrow or Sunday that will correct whatever issues I have introduced in the integration... well before your meeting on Monday.  The techniques are sound that are used.  I don't anticipate significant issues.

_Description_

The commands one can use from the Scala client are:

	  SocketClient.scala <named args...> 
	  where named args are:

	    --cmd startServer   --filePath <filePath> 
	              [--host <hostname or ip> ... default = localhost]
	              [--port <user port no.> ... default=9999]
	    --cmd stopServer  [--host <hostname or ip> ... default = localhost] 
	              [--port <user port no.> ... default=9999]
	    --cmd addModel    --filePath <filePath>
	    --cmd removeModel   --modelName '<modelName>'
	    --cmd serverStatus
	    --cmd executeModel  --modelName '<modelName>' 
	              --msg '<msg data>'

The commands you see above all go across the connection as one string with linefeed delimiters.  For example, the addModel command goes in a formatted string like this:

		s"$cmd\n$modelName\n$modelSrc"

That is, the addModel command on the first line, followed by the model name to be added, followed by the actual python model source file content on subsequent lines.  Python has rich string and collection manipulation like Scala, so lists can be "popped" by line index which will remove the line for that index.  You will see a lot of pop(0) in the code.

The python files are added to a subdirectory of one that is declared in the PYTHONPATH environment variable.  This must be set whereever you test.  For example, if you were to copy the Python folder to your home directory...

	export PYTHONPATH=$HOME/python

The loading of the modules has been tested separately without problems.  We can add arbitrary models to the same python server dynamically.  Both the server commands and the models use this loading technique.  The function importName(moduleName, name) on line 32 of the server does the job.

_Notes_

1) The files:

	rich@pepper:~/github1/dev/r1.5.0/kamanja/trunk/FactoriesOfModelInstanceFactory/PythonModelPrototype/src/main$ ls -lR
	.:
	total 12
	drwxrwxr-x 5 rich rich 4096 May 28 00:50 python
	drwxrwxr-x 2 rich rich 4096 May 25 21:54 resources
	drwxrwxr-x 2 rich rich 4096 May 28 00:47 scala

	./python:
	total 16
	drwxrwxr-x 2 rich rich 4096 May 28 00:49 commands
	drwxrwxr-x 2 rich rich 4096 May 28 00:49 models
	drwxrwxr-x 2 rich rich 4096 May 28 00:53 modelsToLoad
	-rw-rw-r-- 1 rich rich 3727 May 28 00:49 pythonserver.py

	./python/commands:
	total 20
	-rw-rw-r-- 1 rich rich 1036 May 28 00:49 addModel.py
	-rw-rw-r-- 1 rich rich  394 May 28 00:49 executeModel.py
	-rw-rw-r-- 1 rich rich  268 May 28 00:49 removeModel.py
	-rw-rw-r-- 1 rich rich  187 May 28 00:49 serverStatus.py
	-rw-rw-r-- 1 rich rich   95 May 28 00:49 stopServer.py

	./python/models:
	total 0

	./python/modelsToLoad:
	total 16
	-rwx------ 1 rich rich 145 May 28 00:49 add.py
	-rwx------ 1 rich rich 168 May 28 00:49 divide.py
	-rwx------ 1 rich rich 162 May 28 00:49 multiply.py
	-rwx------ 1 rich rich 246 May 28 00:49 subtract.py

	./resources:
	total 0

	./scala:
	total 16
	-rw-rw-r-- 1 rich rich 15094 May 28 00:47 SockClient.scala

2) The scala script is the client serving as the proxy for the python stub model, python factory, and python factory of factories.

3) The python server program is located in the python folder.  The python commands that handle command messages sent from the scala client are found in the python commands directory.  They are loaded in pythonserver.py:67

4) The server commands are: 'addModel', 'removeModel', 'serverStatus', 'executeModel', 'stopServer'.  Notice that the names of these commands **_EXACTLY_** match the main stem of the python source files in the _commands_ subdirectory.  This is important.  As written the file name is used as the command name when building the function dispatch dictionary in the server.

5) The _modelsToLoad_ directory contain the sample models to test the server.  It is these files that you will add to the server.  Currently there is an add, divide, multiply and subtract "model" available.  They all take a list of numbers as their principal argument and return result only from it.  The _models_ directory is where the model source will land for the addModel command.  The first path in PYTHONPATH's value is assumed writable and used to construct the path for the path/_models/$modelName_.py file.

6) There are some (but not enough) semantic checking for arguments.  On the server side and in the model implementations, it will be quite easy to cause the python to throw exceptions.  Any demo (if it is your intention to do that) should have prepared and tested commands with admonishment that this is just a prototype, blah blah.  Of special note is that there are no state transition type checks.  If you failed to add a server before trying to add a model... o well.

7) All server commands share the argument list. For example, here is the executeModel.py command:

	# ExecuteModelCmd message is formatted like this Scala string : s"$cmd\n$modelName\n$msg"
	class Handler(object): 
		def handler(self, modelDict, host, port, cmdList):
			modelName = cmdList.pop(0).strip()
			# assumption for this revision is that the message is one line of CSV data.
			msg = cmdList.pop(0).strip().split(',')
			cmd = modelDict[modelName]
			results = cmd.handler(msg)
			return results

The _$cmd_ in the comment is the executeModel key that causes the server command dispatcher to send control here.  The _modelName_ is the one that will execute.  Its name is used to find the appropriate model in the model dispatch map.  The _msg_, for this version at least, assumes just one line to be consumed.  That could be relaxed for real use.  The goal here is simply to demonstrate the patterns of communication.

8) Like the server commands, the models are dispatched by name lookup where the key is the modelName.  Here is an example of one of our test models, add.py:

	class Handler(object): 
		def handler(self, numbers):
			sumofTup = int(numbers.pop(0))
			for v in numbers
				sumofTup += int(v)
			return str(sumofTup)

As you can see, there currently is very little in the way of type checking.  The parameter, _numbers_, is a list of strings.  Again, this is to demonstrate the patterns of communication, not provide a robust example that manages malformed inputs and the rest.

9) This version of the python server simply accepts a list of strings as the input to the model.  A kamanja message might be represented differently.  A mapped representation can be easily "serialized" to a list of tuple2 and back again.  This might be the interim solution until full type support, server access to the metadata, etc is completed and integrated.

10) If you don't have an __init__.py file in your package directories, the dynamic loader will __fail__.  Here is a good explanation from [http://python-notes.curiousefficiency.org/en/latest/python_concepts/import_traps.html#the-missing-init-py-trap]

The missing __init__.py trap

This particular trap applies to 2.x releases, as well as 3.x releases up to and including 3.2.

Prior to Python 3.3, filesystem directories, and directories within zipfiles, had to contain an __init__.py in order to be recognised as Python package directories. Even if there is no initialisation code to run when the package is imported, an empty __init__.py file is still needed for the interpreter to find any modules or subpackages in that directory.

This has changed in Python 3.3: now any directory on sys.path with a name that matches the package name being looked for will be recognised as contributing modules and subpackages to that package.

11) 