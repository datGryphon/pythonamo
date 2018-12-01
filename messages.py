import pickle
import struct

############################################
'''	Message Codes (first byte of message)

    00 -- clientMessage

    01 -- REQMessage
    
    10 -- NewViewMessage

    02 -- clientConnectRequest

    03 -- clientPut

    30 -- clientPutResponse

    30 -- putResponse

    04 -- clientGet

    40 -- clientGetResponse

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
def _unpack_message(data):
    message_type = data[0]
    message = pickle.loads(data[5:])

    return bytes([message_type]), message


def client_message(user_input):
    data = pickle.dumps(user_input)
    return b'\x00' + struct.pack('!i', len(data)) + data


# Operation: 1 - add peer, 2 - delete peer
def reqMessage(view_id, req_id, operation, address):
    data = pickle.dumps((view_id, req_id, operation, address))
    return b'\x01' + struct.pack('!i', len(data)) + data


def okMessage(view_id, req_id):
    data = pickle.dumps((view_id, req_id))
    return b'\xff' + struct.pack('!i', len(data)) + data


def newView(view_id, address):
    data = pickle.dumps((view_id, address))
    return b'\x10' + struct.pack('!i', len(data)) + data


def clientConnectReq():
    return b'\x02' + struct.pack('!i', 0)


def putMessage(name, value, context):
    data = pickle.dumps((name, value, context))
    return b'\x03' + struct.pack('!i', len(data)) + data


def putResponse(name, value, context):
    data = pickle.dumps((name, value, context))
    return b'\x30' + struct.pack('!i', len(data)) + data


def getMessage(name):
    data = pickle.dumps(name)
    return b'\x04' + struct.pack('!i', len(data)) + data

# send back the file name and the combined list of values
def getResponse(name, result):
    data = pickle.dumps((name, result))
    return b'\x40' + struct.pack('!i', len(data)) + data


def clientRemNode(name):
    data = pickle.dumps(name)
    return b'\x06' + struct.pack('!i', len(data)) + data


def storeFile(name, value, context, stamp):
    data = pickle.dumps((name, value, context, stamp))
    return b'\x07' + struct.pack('!i', len(data)) + data


def storeFileResponse(name, value, context, stamp):
    data = pickle.dumps((name, value, context, stamp))
    return b'\x70' + struct.pack('!i', len(data)) + data


def getFile(name, stamp):
    data = pickle.dumps((name, stamp))
    return b'\x08' + struct.pack('!i', len(data)) + data


def getFileResponse(name, result, stamp):
    data = pickle.dumps((name, result, stamp))
    return b'\x80' + struct.pack('!i', len(data)) + data


def peerList(peers):
    data = pickle.dumps(peers)
    return b'\x09' + struct.pack('!i', len(data)) + data


def forwardedReq(msg):
    data = pickle.dumps(msg)
    return b'\x0A' + struct.pack('!i', len(data)) + data


def responseForForward(msg):
    data = pickle.dumps(msg)
    return b'\x0B' + struct.pack('!i', len(data)) + data


def _get_payload_len(len_str):
    return struct.unpack('!i', len_str)[0]
