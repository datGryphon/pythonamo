class Request(object):
	
	def __init__(self,rtype, args, sendBackTo):
		self.type=rtype	
		self.sendBackTo=sendBackTo
		self.responses={}

		if rtype == 'put':
			self.forwardedTo=None
			self.hash=args[0]
			self.value=args[1]
			self.value=args[2]
		elif rtype == 'get':
			self.forwardedTo=None
			self.hash=args
			self.value=None
			self.value=None
		elif rtype == 'for_put':
			self.forwardedTo=args[0]
			self.hash=args[1]
			self.value=args[2]
			self.value=args[3]
		else:
			self.forwardedTo=args[0]
			self.hash=args[1]
			self.value=None
			self.value=None