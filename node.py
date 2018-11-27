import socket
from collections import defaultdict
from storage import Storage
from ring import Ring
from math import floor
import select
import sys

import messages


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
            self.join_phase()


    def join_phase(self):
        #tcp connect to Peer hostname
        peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer.settimeout(120)

        #write down peer address incase you need to contact him later
        self.peer_server_addresses.append((self.leader_hostname,self.leader_port))

        try:
            peer.connect((self.leader_hostname,self.leader_port))
        except socket.timeout:
            print("Error! could not connect to bootstrap peer")
            exit(0)

        #send newPeer req
        peer.sendall(messages.newPeerReq(self.my_address))
        #add peer socket to peerlist key is the hostname (not the full addr tuple)
        self.peer_sockets[peer.getpeername()[0]]=peer

        while True:
            #if response from peer read it
            readable_peers=select.select(
                    list(self.peer_sockets.values()),[],[],0
                )[0]

            if readable_peers:
                sock=readable_peers[0]
                msg=sock.recv(5)

                if msg == '':
                    print("Peer closed connection: %s"%(sock.getpeername()))
                    #remove sock from conns
                    exit(0)
                elif len(msg) == 5:
                    if msg[0] == '\x08':
                        pass
                    #if peer list
                        #create list of peers
                        #mark each peers state as unconnected,unsynched

                        #in loop ten times
                            #connect to each peer with a short time out
                                #if connected, add socket to peerlist,
                                    #mark peer as connected
                        #if not connected to all peers after loop, abort

                        #send newPeer req to all peers
                    elif msg[0] == '\x06':
                        pass
                    #if storeFile req, store and ack

                    elif msg[0] == '\xff':
                        pass
                    #if all_done, mark peer as synched
                        #if all peers are synched
                            #send Join_Done to all peers
                                #if a peer closed connection, set reminder to reconnect to it
                            #enter main loop
                    else: 
                else:
                    print("Incomplete message header received, error!")
                    exit(0)

    def main_loop(self):

        self.ongoing_requests=[]

        print("entered main loop")
        print("++>")
        sys.stdout.flush()

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

                        elif msg[0] == '\x02'
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
                #if new message from conn
                    #read it and process command

                    #if its a forwarded request
                        #Extract the contained message

                        #call the correct request handling code (put or get)
                            #give sendBackTo=sock.getpeername() as an argument

                    #elif its a storeFile req
                    #elif its a I stored that response

                    #elif its a getFile req
                    #elif its a getFile response

                    #elif remove node


            #use select to check if you can read from user input
            readable=select.select([sys.stdin.fileno()],[],[],0)[0]

            if readable:
                user_cmd=input()
                #pass argument that says sendBackTo=stdin
                self.start_request(user_cmd)


            #if a peer comes back online
                #check if there are any hinted handoffs that need to be handled

            #Lets save this for last, I'm going to make it so that as part of
            #the main event loop, the server reads 'client' commands from stdin
            #so each node instance also functions as a client
            #use select to check client conns for message
                #if new message, process command

    def start_request(self, user_cmd, sendBackTo='stdin'):

    def _process_message(self, data, sender):
        data_tuple = messages._unpack_message(data)
        if data_tuple[0] == 0:  # Message from client, second element should be user_input string
            result = self._process_command(data_tuple[1])
            print(result)

    def _process_command(self, user_input):
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
        return self.command_registry[command](data)

#changed the way peers are connected to be initiated by the new peer
#So I dont think we need this function
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

    def put_data(self, data):
        """Add K,V pair to the database. data[0] should be key, concat(data[1:]) will be value"""
        if len(data) != 3:
            return "Error: Invalid opperands\nInput: [<key>,<prev version>,<value>]"

        key = data[0]
        prev = data[1]
        value = data[2]
        target_node = self.membership_ring.get_node_for_key(data[0])

        if target_node == self.my_hostname:

            # todo: sloppy quorum

            #send store command to all nodes that should replicate

            #if at least sloppy_W-1 respond
            self.db.storeFile(key, self.my_hostname, prev, value)
            return "stored %s:%s locally [%s]" % (key, value, self.my_hostname)
        
            #otherwise, write failed
        else:
            return self._send_data_to_peer(target_node, key, value)

    def get_data(self, data):
        """Retrieve V for given K from the database. data[0] must be the key"""
        if not data:
            return "Error: key required"

        target_node = self.membership_ring.get_node_for_key(data[0])

        if target_node == self.my_hostname:
            #if sloppy_R == 1
            return "retrieved %s:%s locally [%s]" % (
                data[0], self.db.getFile(data[0]), self.my_hostname
            )

            #otherwise, send read request to all replicas
                #when you get R-1 responses, send each unique response back 
                #to the client

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

    def _send_data_to_peer(self, target_node, key, value):

        #forward request to correct peer

        #if you are one of the replicas, wait for peer message
            #and respond to it

        #wait for correct peer to send you confirmation or failue
            #for the write op

        return "stored %s:%s on node %s" % (key, value, target_node)

    def _get_data_from_peer(self, target_node, key):

        #forward request to correct peer

        #if you are one of the replicas wait for the peer message 
            #and respond to it

        #wait for peer to send back all results or a failure

        return "retrieved %s from node %s" % (target_node, key)

    def _delete_data_from_peer(self, target_node, key):
        return "deleted %s from node %s" % (target_node, key)

    # todo: If a command is given to a follower, forward it to the leader. Come to it if time permits.
    def forward_request_to_leader(self, user_input):
        """When a command is passed to the leader"""
        return "Forwarded request to " + self.leader_hostname
