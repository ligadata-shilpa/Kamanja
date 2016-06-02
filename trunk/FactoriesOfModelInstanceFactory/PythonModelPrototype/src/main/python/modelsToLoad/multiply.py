import abc
from common import ModelInstance
import json

class Handler(ModelInstance): 
	""" Model multiply multiplies one or more numbers and returns the product. """
	""" Model informational methods are found in the ModelInstance"""
	""" superclass. """
	def handler(self, numbers):
		productofTup = int(numbers.pop(0))
		for v in numbers
			productofTup *= int(v)
		return str(productofTup)
