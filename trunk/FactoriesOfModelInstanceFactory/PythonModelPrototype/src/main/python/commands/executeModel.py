
# ExecuteModelCmd looks formatted like this Scala string : s"$cmd\n$modelName\n$msg"
class Handler(object): 
	def handler(self, modelDict, host, port, cmdList):
		modelName = cmdList.pop(0).strip()
		# what is left after popping the model name are messages to process... 1 or more
		# Process each message left in the list
		cmd = modelDict[modelName]
		results = []
		for i, rawMsg in enumerate(cmdList):
			msg = rawMsg.strip().split(',')
			print "processing cmd = (" + i + ") " + cmd + " with arguments " + str(msg)
			res = cmd.handler(msg)
			results.append(res)		
		resultStr = json.dumps(results)
		return resultStr


