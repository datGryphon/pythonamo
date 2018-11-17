from collections import defaultdict

from ring import Ring


class Node(object):

    def __init__(self, is_leader, leader_hostname):

        self.is_leader = is_leader
        self.leader_hostname = leader_hostname

        self.membership_ring = Ring()  # Other nodes in the membership
        if self.is_leader:
            self.membership_ring.add_node(leader_hostname, leader_hostname)

        print(len(self.membership_ring))

        # saves all the pending membership messages
        self._membership_messages = defaultdict(set)

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

        # todo: implement totem?
        # send add message to all peers.
        # expect replies from at least N/2
        # send commit message to all peers

        self.membership_ring.add_node(data[0], data[0])  # node id is same as hostname for now
        return "added " + data[0] + " to ring"

    def remove_node(self, data):
        if not data:
            return "Error: hostname required"

        self.membership_ring.remove_node(data[0])
        return "removed " + data[0] + " from ring"

    def put_data(self, data):
        """Add K,V pair to the database. data[0] should be key, concat(data[1:]) will be value"""
        if not data:
            return "Error: requires a key value pair"

        key = data[0]
        value = " ".join(data[1:])
        target_node = self.membership_ring.get_node_for_key(data[0])
        return self._send_data_to_peer(target_node, key, value)

    def get_data(self, data):
        """Retrieve V for given K from the database. data[0] must be the key"""
        if not data:
            return "Error: key required"

        target_node = self.membership_ring.get_node_for_key(data[0])
        return self._get_data_from_peer(target_node, data[0])

    def delete_data(self, data):
        """Retrieve V for given K from the database. data[0] must be the key"""
        if not data:
            return "Error: key required"

        target_node = self.membership_ring.get_node_for_key(data[0])
        return self._delete_data_from_peer(target_node, data[0])

    def _send_data_to_peer(self, target_node, key, value):
        return "stored %s:%s on node %s" % (key, value, target_node)

    def _get_data_from_peer(self, target_node, key):
        return "retrieved %s from node %s" % (target_node, key)

    def _delete_data_from_peer(self, target_node, key):
        return "deleted %s from node %s" % (target_node, key)

    # todo: If a command is given to a follower, forward it to the leader. Come to it if time permits.
    def forward_request_to_leader(self, user_input):
        """When a command is passed to the leader"""
        return "Forwarded request to " + self.leader_hostname
