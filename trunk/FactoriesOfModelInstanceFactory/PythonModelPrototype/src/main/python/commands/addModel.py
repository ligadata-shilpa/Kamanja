import os
import os.path
import json
from common.CommandBase import CommandBase

class addModel(CommandBase): 
	"""
	The addModel command ...uh... adds models to the server.
	"""

	def __init__(self, pkgCmdName, host, port):
		super(addModel, self).__init__(pkgCmdName, host, port)

	def handler(self, modelDict, host, port, cmdOptions, modelOptions):
		if "ModelName" in cmdOptions:
			modelName = str(cmdOptions["ModelName"])
		else:
			modelName = ""
		#
		if "ModelFile" in cmdOptions:
			modelFileName = str(cmdOptions["ModelFile"])
		else:
			modelFileName = ""
		#
		print "Entered addModel... model to be added = {} ... file = {}".format(modelName,modelFileName)
		
		pypath = modelDict["PythonInstallPath"]
		modelSrcPath = "{}/models/{}".format(pypath,modelFileName)
		print "addModel.handler entered ... modelSrcPath = {}".format(modelSrcPath)
		modelName = cmdOptions["ModelName"]

		result = ""
		inputfields = ""
		outputfields = ""
		reasonablePath = os.path.exists(modelSrcPath) and os.path.isfile(modelSrcPath) and modelName != "" and modelFileName != ""
		if reasonablePath:
			#(parentDir, file) = os.path.split(modelSrcPath)
			moduleName = str.split(modelFileName,'.')[0]  
			print "model to be added = {}.{}".format(moduleName, modelName)
			#all models found in models subdir of the pypath
			HandlerClass = self.importName("models." + moduleName, modelName)
			handler = HandlerClass(str(host), str(port), cmdOptions)
			print "handler produced"
			modelDict[str(modelName)] = handler
			print "model {}.{} added!".format(moduleName, modelName)
			(inputfields, outputfields) = handler.getInputOutputFields()
			modelAddMsg = "model {}.{} added".format(moduleName,modelName)
			result = json.dumps({'Cmd' : 'addModel', 'Server' : host, 'Port' : str(port), 'Result' : modelAddMsg, 'InputFields' : inputfields, 'OutputFields' : outputfields })
		else:
			inputfields = []
			outputfields = []
			modelAddMsg = "ModuleName.ModelName '{}.{}' is invalid...it does not reference a valid class".format(moduleName, modelName)
			result = json.dumps({'Cmd' : 'addModel', 'Server' : host, 'Port' : str(port), 'Result' : modelAddMsg, 'InputFields' : inputfields, 'OutputFields' : outputfields })

		print("AddModel results = {}").format(result)

		return result

	def importName(self, moduleName, name):
		"""
		Import a named object from a module in the context of this function 
		"""
		try:
			print "load model = " + moduleName 
			module = __import__(moduleName, globals(), locals(), [name])
			print "module obtained"
		except ImportError:
			return None
		return getattr(module, name)

