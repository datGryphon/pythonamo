import socket
from collections import defaultdict
from storage import Storage
from ring import Ring
from math import floor

import messages


class Node(object):

    def __init__(self, is_leader, leader_hostname, my_hostname, tcp_port=13337, sloppy_Qfrac=0.34, sloppy_R=1, sloppy_W=3):

        self.is_leader = is_leader
        self.leader_hostname = leader_hostname
        self.my_hostname = my_hostname
        self.tcp_port = tcp_port

        self.membership_ring = Ring()  # Other nodes in the membership
        if self.is_leader:
            self.membership_ring.add_node(leader_hostname, leader_hostname)

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

        self.db = Storage(':memory:')  # set up sqlite table in memory

        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def accept_connections(self):
        self.tcp_socket.bind((self.my_hostname, self.tcp_port))
        self.tcp_socket.listen(5)

        while True:
            conn, addr = self.tcp_socket.accept()
            data = conn.recv(1024)  # can be less than 1024 for this application
            # todo: figure out a more appropriate buffer size
            if not data:
                continue

            self._process_message(data, addr[0])  # addr is a tuple of hostname and port

    def _process_message(self, data, sender):
        data_tuple = messages._unpack_message(data)
        if data_tuple[0] == 0:  # Message from client, second element should be user_input string
            result = self._process_command(data_tuple[1])
            print(result)

    def _process_command(self, user_input):
        """Process commands if node is the leader. Else, forward it to the leader."""
        if not user_input:
            return ""

        if not self.is_leader:
            return self.forward_request_to_leader(user_input)

        # First word is command. Rest are then arguments.
        command, *data = user_input.split(" ")
        if command not in self.command_registry:
            return "Invalid command"

        # Call the function associated with the command in command_registry
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
