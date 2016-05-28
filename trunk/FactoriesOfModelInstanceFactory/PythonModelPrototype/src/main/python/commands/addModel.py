
import os
import imp

def importName(moduleName, name):
	"""
	Import a named object from a module in the context of this function 
	"""
	try:
		module = __import__(moduleName, globals(), locals(), [name])
	except ImportError:
		return None
	return getattr(module, name)

# AddModelCmd looks formatted like this Scala string : s"$cmd\n$modelName\n$modelSrc"
class Handler(object): 
	def handler(self, modelDict, host, port, cmdList):
		modelName = cmdList.pop().strip()
		# remaining in the cmdList is the python program
		# write it to $PYTHONPATH/models
		modelSrc = str1 = ''.join(cmdList)
		pypathRaw = os.environ['PYTHONPATH']
		pypath = pypathRaw.split(':').pop(0).strip()
		modelPath = '{}/models/{}.py'.format(pypath, modelName)
		modelFile = open(modelPath, 'w')
		modelFile.write(modelSrc)
		modelFile.close()

		#load the model and add it to the dictionary keyed by its name
		HandlerClass = importName("commands." + modelName, "Handler")
		handler = HandlerClass()
		modelDict[modelName] = handler

		return "model added"


