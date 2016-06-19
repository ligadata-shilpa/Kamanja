import abc
from common.ModelInstance import ModelInstance
import json

class SubtractTuple(ModelInstance): 
	""" Model SubtractTuple will subtract msg["b"] from msg["a"] """
	def execute(self, msg):
		""" 
		A real implementation would use the output fields to 
		determine what should be returned. 
		"""
		diffofTups = int(msg["a"])
		diffofTups -= int(msg["b"])
		outMsg = json.dumps({'a' : msg["a"], 'b' : msg["b"], 'result' : diffofTups})
		return outMsg

	def __init__(self, host, port, modelOptions):
		super(SubtractTuple, self).__init__(host, port, modelOptions)

	def getInputOutputFields(self):
		"""The fields and their types are returned  """
		"""This is looking for dict item "TypeInfo" ... really it """
		"""should be some other key... like InputFields and OutputFields"""
		modelOptions = super(SubtractTuple, self).ModelOptions()
		inputFields = dict()
		outputFields = dict()
		if "TypeInfo" in modelOptions:
			inputFields.update(modelOptions["TypeInfo"])
			outputFields.update(modelOptions["TypeInfo"])
			outputFields["result"] = "Int"
		else:
			inputFields["a"] = "Int"
			inputFields["b"] = "Int"
			outputFields["a"] = "Int"
			outputFields["b"] = "Int"
			outputFields["result"] = "Int"

		return (inputFields , outputFields)
