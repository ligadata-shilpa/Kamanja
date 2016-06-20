import os
import os.path
import json
from common.CommandBase import CommandBase

class removeModel(CommandBase): 
	"""
	AddModelCmd input is formatted like this Scala string : 
		s"$cmd\n$modelName\n$modelInfo\n$modelSrc"
	"""
	
	def __init__(self, pkgCmdName, host, port):
		super(removeModel, self).__init__(pkgCmdName, host, port)


	def handler(self, modelDict, host, port, cmdOptions, modelOptions):
		if "ModelName" in cmdOptions:
			modelName = str(cmdOptions["ModelName"])
		else:
			modelName = ""

		if modelName in modelDict:
			del modelDict[modelName]
		else:
			modelKey = modelName
			modelName = "{} was not found in the model dictionary...not".format(modelKey)

		return 'model {} removed'.format(modelName)


