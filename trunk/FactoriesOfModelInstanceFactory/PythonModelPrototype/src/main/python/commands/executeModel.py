
# ExecuteModelCmd looks formatted like this Scala string : s"$cmd\n$modelName\n$msg"
class Handler(object): 
	def handler(self, modelDict, host, port, cmdList):
		modelName = cmdList.pop().strip()
		# assumption for this revision is that the message is one line of CSV data.
		msg = cmdList.pop().strip().split(',')
		cmd = modelDict[modelName]
		results = cmd.handler(msg)
		return results


