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

    def __init__(self, is_leader, leader_hostname, leader_port, my_hostname, tcp_port=13337, sloppy_Qfrac=0.34, sloppy_R=3, sloppy_W=3):

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

        self.sloppy_Qfrac = sloppy_Qfrac  # fraction of total members to replicate on

        # sets self.sloppy_Qsize to the number of replications required
        self.update_SQsize = lambda: self.sloppy_Qsize=floor(len(self.membership_ring) * self.sloppy_Qfrac)
        self.update_SQsize()
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
        self.client_sockets={}
        self.joining_peers={}
        self.peer_sockets={}
        self.peer_server_addresses=[]

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

            #use select to check unidentified conns list
            if self.unidentified_sockets:
                readable=select.select(self.unidentified_sockets,[],[],0)[0]
                #if new message from conn
                if readable:
                    #read it and determine connection type
                    sock=readable[0]
                    msg=sock.recv(5)

                    if msg == '':
                        print("unidentified connection closed.")
                    elif len(msg) == 5:
                        if msg[0] == '\x01':
                        #if new peer, add to joining_peers
                            print("New peer trying to connect %s"%(sock.getpeername()[0]))
                            self.joining_peers[sock.getpeername()[0]]=sock

                            if self.currently_adding_peer:
                                #set random backoff timer
                                pass
                            else:
                                #if not already in the process of adding a peer
                                #ask all peers if you can add a peer
                                pass
                                #set up timeout and a container to store responses

                        elif msg[0] == '\x02':
                        #if new client add to client conns (will do later if time)
                            pass
                    else: 
                        print("Incomplete message header, error?\nHow do I handle this?")
            

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

                        #call the correct request handling code (put or get)
                            #give sendBackTo=sock.getpeername() as an argument
                    #elif its a response to a forwarded request 
                        #update request
                    #elif its a storeFile req
                    #elif its a I stored that response
                        #update_request
                    #elif its a getFile req
                    #elif its a getFile response
                        #updata_request
                    #elif remove node


            #use select to check if you can read from user input
            readable=select.select([sys.stdin.fileno()],[],[],0)[0]

            if readable:
                user_cmd=input()
                #pass argument that says sendBackTo=stdin
                self._process_command(user_cmd)
                print("++>",end='',flush=True)
                
            #if a peer comes back online
                #check if there are any hinted handoffs that need to be handled

            #Lets save this for last, I'm going to make it so that as part of
            #the main event loop, the server reads 'client' commands from stdin
            #so each node instance also functions as a client
            #use select to check client conns for message
                #if new message, process command


    def _process_message(self, data, sender):
        data_tuple = messages._unpack_message(data)
        if data_tuple[0] == 0:  # Message from client, second element should be user_input string
            result = self._process_command(data_tuple[1])
            print(result)

    def _process_command(self, user_input, sendBackTo='stdin'):
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

        # I dont think we need totem, we just need local failure detection
        # if a client sends remove node command, then we will manually
        # transfer necessary files away from the peer, before sending
        # a message to every peer to remove that node, when the node in
        # question gets that final remove message, it should kill itself.

        # likewise, for adding a peer, transfer all necessary files to new peer
        # if peer revceives all the files, send add message to all peers
        # expect replies from at least N/2
        # send commit message to all peers

        self.membership_ring.add_node(data[0], data[0])  # node id is same as hostname for now

        self.update_SQsize()
        # todo: if number of replicas goes up, need to find new peer and send them your files

        return "added " + data[0] + " to ring"

#Send a remove node message to everyone and if you are that node, shutdown
    def remove_node(self, data):
        if not data:
            return "Error: hostname required"

        self.membership_ring.remove_node(data[0])

        self.update_SQsize()
        # todo: update sloppy quorum size, if size changes
        # tell your lowest index replica to delete your files

        return "removed " + data[0] + " from ring"

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
    #   type='for_put', args=(target_node, ('hash','value',{context}))
    #Type can be 'put', 'get', 'for_put', 'for_get'
    #'for_*' if for requests that must be handled by a different peer
    #then when the response is returned, complete_request will send the 
    #output to the correct client or peer (or stdin)
    def start_request(self, rtype, args, sendBackTo='stdin'):
        req = Request(rtype,args,sendBackTo)
        self.ongoing_requests.append(req)


        #send the correct request to the correct peers
        #if get or put
            #send the getFile or storeFile message to everyone in the replication
            #range
        #else, forward the request to the target node
            #send a client getRequest to a peer in the correct range

        if rtype == 'get':
            result = self.db.getFile(args)
            my_resp = getFileResponse(args, result)
            #add my information to the request
            self.update_request(my_resp,self.my_hostname,req)
        elif rtype == 'put':
            self.db.storeFile(args[0],self.my_hostname,args[2],args[1])
            my_resp = storeFileResponse(args[0],args[1],args[2])
            #add my information to the request
            self.update_request(my_resp,self.my_hostname,req)
            
        #return to main event loop


    def update_request(self,msg,sender,request):
        pass
        #after a \x70, \x80 or \x0B is encountered from a peer, this method is called

        #if it is a \x0B
            #remove the message from the payload

            #if sendBackto != stdin
                #send the extracted message to the correct client
            #else, pint the contents of the message to the console

            #remove the ongoing request
        #elif its a \x70
            #set req.responses[sender]=msg

            #number of responses required is min of (sloppy_R/W and the number of replicas)

            #if you have the min number of responses
                #complete_message(req)

        #return to main event loop

    def complete_request(self,request):
        pass

        #if it is a get message:
            #compile the results of the responses
            #into one unique tuple list
        #else, result = 'success'


        #if req.sendbackto == stdin
            #print results to console
        #else
            #create client response message

            #if sendbackto is a peer
                #encapsulate client response inside of a \x0B

            #send message to sendbackto

        #remove request from ongoing list
        #return to main event loop


    def put_data(self, data, sendBackTo='stdin'):
        """Add K,V pair to the database. data[0] should be key, concat(data[1:]) will be value"""
        if len(data) != 3:
            return "Error: Invalid opperands\nInput: [<key>,<prev version>,<value>]"

        key = data[0]
        prev = data[1]
        value = data[2]
        target_node = self.membership_ring.get_node_for_key(data[0])

        if target_node == self.my_hostname:

            # todo: sloppy quorum handled by start_req
            start_request('put',data,sendBackTo=sendBackTo)

            return "started put request for %s:%s locally [%s]" % (key, value, self.my_hostname)
        else:
            return self._send_data_to_peer(target_node,data,sendBackTo)

    def get_data(self, data, sendBackTo='stdin'):
        """Retrieve V for given K from the database. data[0] must be the key"""
        if not data:
            return "Error: key required"

        target_node = self.membership_ring.get_node_for_key(data[0])

        if target_node == self.my_hostname:

            start_request('get',data,sendBackTo=sendBackTo)

            return "started get request for %s:%s locally [%s]" % (
                data[0], self.db.getFile(data[0]), self.my_hostname
            )
        else:
            return self._get_data_from_peer(target_node, data[0])

    def delete_data(self, data):
        """Retrieve V for given K from the database. data[0] must be the key"""
        if not data:
            return "Error: key required"

        target_node = self.membership_ring.get_node_for_key(data[0])

        if target_node == self.my_hostname:
            self.db.remFile(data[0])
            return "deleted %s locally [%s]" % (data[0], self.my_hostname)
        else:
            return self._delete_data_from_peer(target_node, data[0])

    def _send_data_to_peer(self, target_node, data, sendBackTo='stdin'):

        #create for_put request
        start_request('for_put',(target_node,data),sendBackTo=sendBackTo)

        return "forwarded put request for %s:%s to node %s" % (key, value, target_node)

    def _get_data_from_peer(self, target_node, data, sendBackTo='stdin'):

        start_request('for_get',(target_node,data),sendBackTo=sendBackTo)

        return "forwarded get request for %s to node %s" % (target_node, key)

    def _delete_data_from_peer(self, target_node, key):
        return "deleted %s from node %s" % (target_node, key)

    # todo: If a command is given to a follower, forward it to the leader. Come to it if time permits.
    def forward_request_to_leader(self, user_input):
        """When a command is passed to the leader"""
        return "Forwarded request to " + self.leader_hostname
