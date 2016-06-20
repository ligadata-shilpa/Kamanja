import sys
import json
from common.CommandBase import CommandBase

class stopServer(CommandBase):
	"""
	Execute the model mentioned in the cmdOptions feeding it the
	message also found there (key = "InputDictionary")
	"""
	def __init__(self, pkgCmdName, host, port):
		super(stopServer, self).__init__(pkgCmdName, host, port)

	def handler(self, modelDict, host, port, cmdOptions, modelOptions):
		return 'kill-9'
