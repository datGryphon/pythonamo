import pickle
import struct

############################################
'''	Message Codes (first byte of message)

	00 -- clientMessage

	01 -- newPeerRequest

	02 -- clientConnectRequest

	03 -- clientPut

	30 -- putResponse

	04 -- clientGet

	40 -- getResponse

	05 -- getResponse

	06 -- clientRemoveNode

	07 -- storeFile

	70 -- I stored it

	08 -- getFile

	80 -- my results for that file

	09 -- peerList 

	0A -- forwardedClientReq
			contents is the bytestring of the client message

	0B -- ResponseForForwardedClientReq
			contents is the bytestring to be sent to client

	FF -- OK!

'''
############################################
def client_message(user_input):
    return pickle.dumps((0, user_input))

def _unpack_message(data):
    data_tuple = pickle.loads(data)
    return data_tuple

def newPeerReq(address):
	data=pickle.dumps(address)
	return b'\x01'+struct.pack('!i',len(data))+data

def clientConnectReq():
	return b'\x02'+struct.pack('!i',0)

def clientPut(name,value,context):
	data=pickle.dumps((name,value,context))
	return b'\x03'+struct.pack('!i',len(data))+data

def clientGet(name):
	data=pickle.dumps(name)
	return b'\x04'+struct.pack('!i',len(data))+data

#values is the sorted list of (context,value) pairs
def getResponse(values):
	data = pickle.dumps(values)
	return b'\x05'+struct.pack('!i',len(data))+data

def clientRemNode(name):
	data=pickle.dumps(name)
	return b'\x06'+struct.pack('!i',len(data))+data

def storeFile(name,value,context):
	data=pickle.dumps((name,value,context))
	return b'\x07'+struct.pack('!i',len(data))+data

def getFile(name):
	data=pickle.dumps(name)
	return b'\x08'+struct.pack('!i',len(data))+data

def peerList(peers):	
	data=pickle.dumps(peers)
	return b'\x09'+struct.pack('!i',len(data))+data

def forwardedClientReq(msg):
	data=pickle.dumps(msg)
	return b'\x0A'+struct.pack('!i',len(data))+data

def responseForClient(msg):
	data=pickle.dumps(msg)
	return b'\x0B'+struct.pack('!i',len(data))+data

def okMessage():
	return b'\xff'+struct.pack('!i',0)

def _get_payload_len(len_str)
	return struct.unpack('!i',len_str)[0]