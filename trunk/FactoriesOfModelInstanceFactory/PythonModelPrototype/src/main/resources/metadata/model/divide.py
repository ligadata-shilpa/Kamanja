import abc
from common.ModelInstance import ModelInstance
import json

class Handler(ModelInstance): 
	""" Model divide will divide one or more numbers and returns the quotient. """
	""" Should only one number be supplied, the divisor is assumed to be '1'.  """
	""" Model informational methods are found in the ModelInstance superclass. """
	def handler(self, numbers):
		qutotientofTup = int(numbers.pop(0))
		for v in numbers
			qutotientofTup /= int(v)
		return str(qutotientofTup)
