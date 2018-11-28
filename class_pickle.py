from request import Request
import pickle
import sys

if len(sys.argv) != 2:
	print('Error: must provide filename of pickled reqest')
	exit(0)

f=open(sys.argv[1],'wb')

test=Request('put',('hash',"this is a test",{'s1':1}),'leader.name',
		Request('for_put',('correct_node','hash','this is a test',{'s1':1}),
			'client.name',None
		)
	)

print(pickle.dumps(test))
f.write(pickle.dumps(test))