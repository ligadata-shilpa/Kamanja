class Handler(object): 
	def handler(self, modelDict, host, port, whocares):
		svrstatus = 'active models for host {} ({} are {})'.format(host, port, modelDict.keys())
		return svrstatus
