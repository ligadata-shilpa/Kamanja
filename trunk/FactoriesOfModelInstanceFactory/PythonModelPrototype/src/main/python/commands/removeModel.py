
# RemoveModelCmd looks formatted like this Scala string : s"$cmd\n$modelName"
class Handler(object): 
	def handler(self, modelDict, host, port, cmdList):
		modelName = cmdList.pop().strip()
		del modelDict[modelName] 

		return 'model {} removed'.format(modelName)


