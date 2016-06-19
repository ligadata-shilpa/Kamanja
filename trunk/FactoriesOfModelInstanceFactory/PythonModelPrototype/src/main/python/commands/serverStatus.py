class serverStatus(object): 
	def handler(self, modelDict, host, port, cmdOptions, modelOptions):
		svrstatus = 'active models for pyserver host {} ({}) are: {}'.format(host, port, modelDict.keys())
		return svrstatus
