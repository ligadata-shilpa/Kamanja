
# RemoveModelCmd looks formatted like this Scala string : s"$cmd\n$modelName"
class removeModel(object): 
	def handler(self, modelDict, host, port, cmdOptions, modelOptions):
		modelName = cmdList.pop(0).strip()
		del modelDict[modelName] 

		return 'model {} removed'.format(modelName)


