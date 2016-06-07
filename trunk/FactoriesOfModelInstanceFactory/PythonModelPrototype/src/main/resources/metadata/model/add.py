import abc
from common.ModelInstance import ModelInstance
import json

class Handler(ModelInstance): 
	""" Model add adds one or more numbers and returns the sum. """
	""" Model informational methods are found in the ModelInstance"""
	""" superclass. """
	def handler(self, numbers):
		sumofTup = int(numbers.pop(0))
		for v in numbers
			sumofTup += int(v)
		return str(sumofTup)

