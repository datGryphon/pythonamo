from request import Request
import pickle
import sys

if len(sys.argv) != 2:
	print('Error: must provide filename of pickled reqest')
	exit(0)

f=open(sys.argv[1],'rb')

contents=f.read()

print(contents)

req=pickle.loads(contents)

print("REQ %f: %s, (%s,%s,%s), %s, %s"%(
		req.time_created, req.type,
		req.hash, req.value, req.context,
		req.sendBackTo, req.previous_request
	)
)

if req.previous_request:
	print("PREVREQ %f: %s, (%s,%s,%s), %s, %s"%(
			req.previous_request.time_created, req.previous_request.type,
			req.previous_request.hash, req.previous_request.value, req.previous_request.context,
			req.previous_request.sendBackTo, req.previous_request.previous_request
		)
	)
