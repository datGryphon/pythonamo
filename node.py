import socket
from collections import defaultdict
from storage import Storage
from ring import Ring
from math import floor
import select
import sys

import messages
from request import Request


class Node(object):

    def __init__(self, is_leader, leader_hostname, leader_port, my_hostname, tcp_port=13337, sloppy_Qsize=5, sloppy_R=3, sloppy_W=3):

        self.is_leader = is_leader
        self.leader_hostname = leader_hostname
        self.leader_port=leader_port
        self.my_hostname = my_hostname
        self.tcp_port = tcp_port
        self.my_address=(self.my_hostname,self.tcp_port)

        self.membership_ring = Ring()  # Other nodes in the membership
        if self.is_leader:
            self.membership_ring.add_node(leader_hostname, leader_hostname)

        self.currently_adding_peer=False

        self.sloppy_Qsize = sloppy_Qsize  # fraction of total members to replicate on
        #number of peers required for a read or write to succeed.
        self.sloppy_R=sloppy_R
        self.sloppy_W=sloppy_W

        # saves all the pending membership messages
        self._membership_messages = defaultdict(set)
        self.current_view = 0  # increment this on every leader election
        self.request_id = 0  # increment this on every request sent to peers

        # Maps command to the corresponding function.
        # Command arguments are passed as the first argument to the function.
        self.command_registry = {               # Possible commands:
            "add-node": self.register_node,     # 1. add node to membership
            "remove-node": self.remove_node,    # 2. remove node from membership
            "put": self.put_data,               # 3. put data
            "get": self.get_data,               # 4. get data
            "delete": self.delete_data,         # 5. delete data
            "quit": lambda x: x                 # 6. Quit
        }

        #eventually need to change this so table is persistent across crashes
        self.db = Storage(':memory:')  # set up sqlite table in memory

        #create socket
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #bind socket to correct port
        self.tcp_socket.bind(self.my_hostname,self.tcp_port)
        self.tcp_socket.listen(5)

        #lists of connections
        self.unidentified_sockets=[]
        self.peer_sockets={}
        self.client_sockets={}

    def accept_connections(self):
        #moved to __init__
        # self.tcp_socket.bind((self.my_hostname, self.tcp_port))
        # self.tcp_socket.listen(5)

        while True:
            conn, addr = self.tcp_socket.accept()
            data = conn.recv(1024)  # can be less than 1024 for this application
            # todo: figure out a more appropriate buffer size
            if not data:
                continue

            self._process_message(data, addr[0])  # addr is a tuple of hostname and port

    def start(self):

        if is_leader:
            self.main_loop()
        else:
            self.accept_connections()

    def main_loop(self):

        self.ongoing_requests=[]

        print("entered main loop")
        print("++>",end='',flush=True)

        while True:

            #use select to check if server socket has inbound conn
            if select.select([self.tcp_socket],[],[],0)[0]:
                #if so, accept connection
                (sock,addr) = self.tcp_socket.accept()
                #put on unidentified conns list
                self.unidentified_sockets.append(sock)

            #use select to check peer conns for message
            if self.peer_sockets:
                readable=select.select(
                    list(self.peer_sockets.values()),[],[],0
                )[0]

                if readable:
                    pass
                #if new message from conn
                    #read it and process command

                    #if its a forwarded request
                        #Extract the contained message

                        #start_request(type,args,sendbackto=sock.getpeername()[0])
                    #elif its a response to a forwarded request 
                        #update request
                    #elif its a storeFile req
                        #take action and acknowledge
                    #elif its a StoreFileResponse
                        #update_request
                    #elif its a getFile req
                        #get result and respond
                    #elif its a getFileResponse
                        #updata_request
                    #elif add node
                        #call self.register_node
                    #elif remove node
                        #call self.remove_node
                
            #if a peer comes back online
                #check if there are any hinted handoffs that you need to get

            #use select to check client conns for message
                #if new message, process command



    def _process_message(self, data, sender):
        data_tuple = messages._unpack_message(data)
        if data_tuple[0] == 0:  # Message from client, second element should be user_input string
            result = self._process_command(data_tuple[1],sendBackTo=sender)
            print(result)

    def _process_command(self, user_input, sendBackTo):
        """Process commands"""
        if not user_input:
            return ""

        # no leader anymore, after a node has been boostrapped, being the leader means nothing
        # if not self.is_leader:
        #     return self.forward_request_to_leader(user_input)

        # First word is command. Rest are then arguments.
        command, *data = user_input.split(" ")
        if command not in self.command_registry:
            return "Invalid command"

        # Call the function associated with the command in command_registry
        if command == 'put' or command == 'get':
            return self.command_registry[command](data,sendBackTo)
        else:
            return self.command_registry[command](data)

    def register_node(self, data):
        """Add node to membership. data[0] must be the hostname"""
        if not data:
            return "Error: hostname required"

        if len(self.membership_ring) == 1:  # Only leader is in the ring. Just add.
            self.membership_ring.add_node(data[0], data[0])  # node id is same as hostname for now
            return "added " + data[0] + " to ring"

        self.membership_ring.add_node(data[0], data[0])  # node id is same as hostname for now

        self.update_SQsize()
        # todo: if number of replicas goes up, need to find new peer and send them your files

        return "added " + data[0] + " to ring"

#Send a remove node message to everyone and if you are that node, shutdown
    def remove_node(self, data):
        if not data:
            return "Error: hostname required"

        self.membership_ring.remove_node(data[0])

        return "removed " + data[0] + " from ring"


    #this is where we need to handle hinted handoff if a 
    #peer is not responsive by asking another peer to hold the
    #message until the correct node recovers
    def send_to_replicas(nodes,msg):
        for node in nodes:
            self.connections[node].sendall(msg)

    #request format:
    #object which contains 
        #type
        #sendBackTo
        #forwardedTo =None if type is not for_*
        #hash
        #value =None if type is get or forget
        #context =None if type is get or forget
        #responses = { sender:msg, sender2:msg2... }

    #args format is determined by type:
    #   type='get', args='hash'
    #   type='put', args=('hash','value',{context})
    #   type='for_get', args=(target_node,'hash')
    #   type='for_put', args=(target_node, 'hash','value',{context})
    #Type can be 'put', 'get', 'for_put', 'for_get'
    #'for_*' if for requests that must be handled by a different peer
    #then when the response is returned, complete_request will send the 
    #output to the correct client or peer (or stdin)
    def start_request(self, rtype, args, sendBackTo,prev_req=None):
        req = Request(rtype,args,sendBackTo)    #create request obj
        self.ongoing_requests.append(req)       #set as ongoing

        target_node= self.membership_ring.get_node_for_key(req.hash)
        replica_nodes=self.membership_ring.get_replicas_for_key(req.hash)

        #Find out if you can respond to this request
        if rtype == 'get':
            data_nodes = self.membership_ring.get_node_for_key(
                args,self.sloppy_Qsize-1
            )
            #add my information to the request
            result = self.db.getFile(args)
            my_resp = getFileResponse(args, result, req.time_created)
            self.update_request(my_resp,self.my_hostname,req)
            #send the getFile message to everyone in the replication range
            msg = getFile(req.hash, req.time_created)
            #this function will need to handle hinted handoff
            self.send_to_replicas(replica_nodes,msg)
        elif rtype == 'put':
            data_nodes = self.membership_ring.get_node_for_key(
                args[0],self.sloppy_Qsize-1
            )
            self.db.storeFile(args[0],self.my_hostname,args[2],args[1])
            my_resp = storeFileResponse(args[0],args[1],args[2])
            #add my information to the request
            self.update_request(my_resp,self.my_hostname,req)
            #send the storeFile message to everyone in the replication range
            msg = storeFile(req.hash,req.value,req.context,req.time_created)
            #this function will need to handle hinted handoff
            self.send_to_replicas(replica_nodes,msg)
        else:
            msg = forwardedReq(req)
            #forward message to target node
            self.connections[req.forwardedTo].sendall(msg)

    def find_req_for_msg(self,msg,sender, req_ts=None):
        if not req_ts:
            contents=_unpack_message(msg[5:])
            #find correct request corresponding to message
            #by checking the response id (aka timestamp of the req creation)
            if msg[0] == '\x70':
                req_ts = contents[3]
            elif msg[0] == '\x80':
                req_ts = contents[2]
            #or by checking matching the response id of the msg's req's prev_req.time
            else:  
                req_ts = contents.previous_request.time_created

        return list(filter(
            lambda r: r.time_created == req_ts, self.ongoing_requests
        ))

    #after a \x70, \x80 or \x0B is encountered from a peer, this method is called
    def update_request(self,msg,sender,request=None):
        if not request:
            request=self.find_req_for_msg(msg,sender)
            if not request:
                print("Could not find request for message, %s was probably too slow"%(sender))
                return
            request=request[0]

        request.responses[sender]=msg
        if msg[0] == '\x0B':
            self.complete_request(request)
        else:#its a \x70 or \x80
            minNumResp = self.sloppy_R if msg[0] == '\x80' else self.sloppy_W 

            if len(request.responses) >= minNumResp:
                self.complete_message(req)

    def coalesce_responses(self,request):
        resp_list=list(request.responses.values())
        results=[]
        for resp in resp_list:
            results.extend([
                tup for tup in _unpack_message(resp[5:])[1] if tup not in results
            ])
        return results

    def complete_request(self,request):
        if request.type == 'get':
            #if sendbackto is a peer
            if request.sendBackTo in self.membership_ring:
                #this is a response to a for_*
                #send the whole request object back to the peer
                msg = responseForForward(request)
            else:
                #compile results from responses and send them to client
                #send message to client
                msg = getResponse(request.hash,self.coalesce_responses(request))
        elif request.type == 'put':
            if request.sendBackTo in self.membership_ring:
                #this is a response to a for_*
                #send the whole request object back to the peer
                msg = responseForForward(request)
            else:
                #send success message to client
                msg = putResponse(request.hash,request.value,request.context)
        else: #request.type == for_*
            #unpack the forwarded request object
            data = _unpack_message(request.response.values()[0][5:])
            #if sendbackto is a peer
            if request.sendBackTo in self.membership_ring:
                #unpickle the returned put request
                data.previous_request = data.previous_request.previous_request
                #send the response object you got back to the peer
                    #from request.responses (it is the put or get they need)
                    #if you need to, make req.prev_req = req.prev_req.prev_req
                    #so it looks like you did the request yourself
                msg = responseForForward(data)
            elif request.type == 'for_put':
                msg = putResponse(request.hash,request.value,request.context)
            else: #for_get
                msg = getResponse(request.hash,self.coalesce_responses(data))

        #send msg to request.sendBackTo
        self.connections[request.sendBackTo].sendall(msg)

        #remove request from ongoing list
        self.ongoing_requests = list(filter(
            lambda r: r != request, self.ongoing_requests
        ))

    def put_data(self, data, sendBackTo):
        if len(data) != 3:
            return "Error: Invalid opperands\nInput: (<key>,<prev version>,<value>)"

        key = data[0]
        prev = data[1]
        value = data[2]
        target_node = self.membership_ring.get_node_for_key(data[0])

        if not self.is_leader:
            #forward request to leader for client
            return self._send_data_to_peer(self.leader_hostname,data,sendBackTo)
        else: #I am the leader
            if target_node == self.my_hostname:
                # I'm processing a request for a client directly
                self.start_request('put',data,sendBackTo=sendBackTo)
                return "started put request for %s:%s locally [%s]" % (key, value, self.my_hostname)
            else:# I am forwarding a request from the client to the correct node
                return self._send_data_to_peer(target_node,data,sendBackTo)

    def get_data(self, data, sendBackTo='stdin'):
        """Retrieve V for given K from the database. data[0] must be the key"""
        if not data:
            return "Error: key required"

        target_node = self.membership_ring.get_node_for_key(data[0])

        #if I can do it myself
        if target_node == self.my_hostname:
            #I am processing a request for a client directly
            self.start_request('get',data,sendBackTo=sendBackTo)
            return "started get request for %s:%s locally [%s]" % (
                data[0], self.db.getFile(data[0]), self.my_hostname
            )
        else:#forward the client request to the peer incharge of req
            return self._get_data_from_peer(target_node, data[0])

    def handle_forwarded_req(self,prev_req,sendBackTo):
        target_node = self.membership_ring.get_node_for_key(prev_req.hash)
        #someone forwarded you a put request
        #if you are the leader, check if you can takecare of it, else, 
        #start a new put request with this request as the previous one
        if prev_req.type == 'put' or prev_req.type == 'for_put':
            if self.is_leader:
                if target_node == self.my_hostname:
                    args=(prev_req.hash, prev_req.value, prev_req.context)
                    self.start_request('put',args,sendBackTo,previous_request=prev_req)
                    return "handling forwarded put request locally"
                else:
                    args=(target_node,prev_req.hash, 
                        prev_req.value, prev_req.context
                    )
                    self.start_request('for_put',args,sendBackTo,prev_req)
                    return "Forwarded Forwarded put request to correct node"
            else: #the leader is forwarding you a put
                args=(prev_req.hash, prev_req.value, prev_req.context)
                self.start_request('put',args,sendBackTo,previous_request=prev_req)
                return "handling forwarded put request locally"
        #someone forwarded you a get request, you need to take care of it
        #start new get request with this as the previous one
        else: #type is get or for_get
            start_request('get',prev_req.hash,sendBackTo,prev_req)
            return "handling forwarded get request locally"

    def _send_data_to_peer(self, target_node, data, sendBackTo='stdin'):

        #create for_put request
        start_request('for_put',(target_node,)+data,sendBackTo=sendBackTo)

        return "forwarded put request for %s:%s to node %s" % (key, value, target_node)

    def _get_data_from_peer(self, target_node, data, sendBackTo='stdin'):

        start_request('for_get',(target_node,data),sendBackTo=sendBackTo)

        return "forwarded get request for %s to node %s" % (target_node, key)