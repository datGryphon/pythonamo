import pickle
import struct

############################################
'''	Message Codes (first byte of message)

	00 -- clientMessage

	01 -- newPeerRequest

	02 -- clientConnectRequest

	03 -- clientPut

	04 -- clientGet

	05 -- clientRemoveNode

	06 -- storeFile

	07 -- getFile

	08 -- peerList

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

def clientRemNode(name):
	data=pickle.dumps(name)
	return b'\x05'+struct.pack('!i',len(data))+data

def storeFile(name,value,context):
	data=pickle.dumps((name,value,context))
	return b'\x06'+struct.pack('!i',len(data))+data

def getFile(name):
	data=pickle.dumps(name)
	return b'\x07'+struct.pack('!i',len(data))+data

def peerList(peers):	
	data=pickle.dumps(peers)
	return b'\x08'+struct.pack('!i',len(data))+data

def okMessage():
	return b'\xff'+struct.pack('!i',0)

def _get_payload_len(len_str)
	return struct.unpack('!i',len_str)[0]