import abc
from common import ModelInstance
import json

class Handler(ModelInstance): 
	""" Model subract subtracts one or more numbers from the first supplied. """
	""" The difference is returned """
	""" Model informational methods are found in the ModelInstance superclass. """
	def handler(self, numbers):
		diffofTup = int(numbers.pop(0))
		for v in tuple(numbers)
			diffofTup -= int(v)
		return str(diffofTup)
