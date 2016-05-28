# could check for numbers with isinstance(int_var, int) or isinstance(long_var, long)
class Handler(object): 
	def handler(self, numbers):
		diffofTup = int(numbers.pop(0))
		for v in tuple(numbers)
			diffofTup -= int(v)
		return str(diffofTup)
