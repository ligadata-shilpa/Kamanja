	def execute(msg):
		sumofTup = int(msg["a"])
		sumofTup += msg["b"]
		return str(sumofTup)
====
	x = 0
	def init(options):
		x = options["a"]

	def execute(msg):
		sumofTup = x
		sumofTup += msg["b"]
		return str(sumofTup)

====
class AddTuple(object): 
	""" Model add adds one or more numbers and returns the sum. """
	""" Model informational methods are found in the ModelInstance"""
	""" superclass. """
	def execute(self, msg):
		sumofTup = int(msg["a"])
		sumofTup += msg["b"]
		return str(sumofTup)
	
	def init(self, options):
		self.x = options["a"]

====
import abc
from common.ModelInstance import ModelInstance
import json

class AddTuple(ModelInstance): 
	""" Model add adds one or more numbers and returns the sum. """
	""" Model informational methods are found in the ModelInstance"""
	""" superclass. """
	def execute(self, msg)
		sumofTup = int(msg["a"])
		sumofTup += msg["b"]
		return str(sumofTup)


    def __init__(self, modelOptions)
        super.__init__(self,)

1) annotate tasks/ break apart into finer portions if possible, 

2) fix loop on server, 

3) add exception handling to server,

4) fix message sent ... begin marker, protocol version, checksum, cmd payload length, cmd payload, end marker, 

5) fix sample models to consume dictionary values passed at model instantiation, 

6) fix msg at execute to be dictionary as well (this will be created or extracted from scala msg instance on the proxy side and made into json dict for now)
