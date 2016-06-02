
# ExecuteModelCmd looks formatted like this Scala string : s"$cmd\n$modelName\n$msg"
class Handler(object): 
	def handler(self, modelDict, host, port, cmdList):
		modelName = cmdList.pop(0).strip()
		# assumption for this revision is that the message is one line of CSV data.
		msg = cmdList.pop(0).strip().split(',')
		cmd = modelDict[modelName]
		print "processing cmd = " + cmd + " with arguments " + str(msg)
		results = cmd.handler(msg)
		return results


