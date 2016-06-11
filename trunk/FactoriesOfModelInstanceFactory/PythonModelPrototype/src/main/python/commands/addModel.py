import os
import os.path
class addModel(object): 
	"""
	AddModelCmd input is formatted like this Scala string : 
		s"$cmd\n$modelName\n$modelInfo\n$modelSrc"
	"""
	def handler(self, modelDict, host, port, modelProperties):
		if "modelName" in cmdOptions:
			modelName = cmdOptions["modelName"]
		else:
			modelName = ""
		# The cmdList contains the python program... write it 
		# to $PYTHONPATH/models.  we could write them one at a time or 
		# do this and join the list again into one string.
		modelSrcPath = cmdOptions["modelSrcPath"]
		pypath = cmdOptions["PythonInstallPath"]
		modelPath = '{}/models/{}.py'.format(pypath, modelName)
		modelFile = open(modelPath, 'w')
		modelFile.write(modelSrc)
		modelFile.close()

		result = ""
		reasonablePath = os.path.exists(modelSrcPath) and os.path.isfile(modelSrcPath)
		if reasonablePath:
			(parentDir, file) = os.path.split(modelSrcPath)
			stem = str.split(file,'.')[0]  # FIXME : we could insist on .py suffix; 
			ok = modelName == "" or stem == modelName
			if not ok:
				result = 'the modelName in the dictionary ({}) and the python model file stem ({}) must be equivalent... models are found by the module name stem (e.g., the "foo" part of "foo.py")'.format(modelName, stem)
			else:

				#load the model and add it to the dictionary keyed by its name
				HandlerClass = importName("models." + modelName, "Handler")
				handler = HandlerClass()
				modelDict[modelName] = handler
				result = "model {} added".format(modelName)

		return "model added"

	def importName(moduleName, name):
		"""
		Import a named object from a module in the context of this function 
		"""
		try:
			print "load model = " + moduleName 
			module = __import__(moduleName, globals(), locals(), [name])
		except ImportError:
			return None
		return getattr(module, name)

