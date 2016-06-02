class Handler(ModelInstance): 
	import os
	import imp

	"""
	AddModelCmd inpur ia formatted like this Scala string : 
		s"$cmd\n$modelName\n$modelInfo\n$modelSrc"
	"""
	def handler(self, modelDict, host, port, cmdList):
		modelName = cmdList.pop(0).strip()
		# The cmdList contains the python program... write it 
		# to $PYTHONPATH/models.  we could write them one at a time or 
		# do this and join the list again into one string.
		modelSrc = str1 = ''.join(cmdList)
		pypathRaw = os.environ['PYTHONPATH']
		pypath = pypathRaw.split(':').pop(0).strip()
		modelPath = '{}/models/{}.py'.format(pypath, modelName)
		modelFile = open(modelPath, 'w')
		modelFile.write(modelSrc)
		modelFile.close()

		#load the model and add it to the dictionary keyed by its name
		HandlerClass = importName("models." + modelName, "Handler")
		handler = HandlerClass()
		modelDict[modelName] = handler

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

