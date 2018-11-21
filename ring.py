from hashlib import md5
import bisect


# Reasonably dumb implementation to get started. vnodes may not work as expected when > 1.
# todo: implement a proper hash ring with support for virtual nodes
class Ring(object):

    def __init__(self, vnodes=1):
        """Create a new Ring.

        :param vnodes: number of vnodes.
        """
        self.vnodes = vnodes
        self._keys = []
        self._nodes = {}

        self._hash = lambda key: int(md5(key.encode('utf-8')).hexdigest(), 16)

    def _repl_iterator(self, node_id):
        """Given a node id, return an iterable of vnode hashes."""

        return (self._hash("%s:%s" % (node_id, i)) for i in range(self.vnodes))

    def __setitem__(self, node_id, node):
        """Add a node, given its id and hostname.

        The given node id is hashed among the number of vnodes.

        """
        for hash_ in self._repl_iterator(node_id):
            if hash_ in self._nodes:
                raise ValueError("Node id %r is already present" % node_id)
            self._nodes[hash_] = node
            bisect.insort(self._keys, hash_)

    def __delitem__(self, node_id):
        """Remove a node, given its id."""

        for hash_ in self._repl_iterator(node_id):
            # will raise KeyError for nonexistent node name
            del self._nodes[hash_]
            index = bisect.bisect_left(self._keys, hash_)
            del self._keys[index]

    #modified so that when number of replicas greater than 0
    #returns list of all nodes that should store the data
    #if n_rep == 0 functions as it did previously
    def __getitem__(self, key, n_rep=0):
        """Return a node hostname, given a key.

        The vnode with a hash value nearest but not less than that of the given
        name is returned. If the hash of the given name is greater than the greatest
        hash, returns the lowest hashed node.

        """
        hash_ = self._hash(key)
        start = bisect.bisect(self._keys, hash_)
        if start == len(self._keys):
            start = 0

        if n_rep != 0:
            #if there are replicas, return the n_rep+1 nodes 
            #starting with keys[start]
            #add the list of keys to itself so the slice wraps around
            return [ self._nodes[k] for k in (self._keys+self._keys)[start:start+n_rep+1] ]
        else: #otherwise just send back the first node
            return self._nodes[self._keys[start]]

    def __len__(self):
        return len(self._nodes) // self.vnodes  # to account for vnodes

    # Helper functions to expose stable API
    def add_node(self, node_id, node_hostname):
        return self.__setitem__(node_id, node_hostname)

    def remove_node(self, node_id):
        return self.__delitem__(node_id)

    #modified so that when number of replicas greater than 0
    #returns list of all nodes that should store the data
    #if n_rep == 0 functions as it did previously
    def get_node_for_key(self, key, n_rep=0):
        return self.__getitem__(key,n_rep)


if __name__ == '__main__':
    r = Ring()

    r.add_node('node1', "node1.hostname")
    r.add_node('node2', "node2.hostname")
    r.add_node('node3', "node3.hostname")
    r.add_node('node4',"node4.hostname")

    # Try inserting a key
    target_hostname = r.get_node_for_key("key4")
    print(target_hostname)  # got node2hostname
    # proceed to put data in target_hostname

    target_hostname = r.get_node_for_key("key2")
    print(target_hostname)  # got node3hostname

    target_hostname = r.get_node_for_key("key6")
    print(target_hostname)  # got node3hostname

    target_hostname = r.get_node_for_key("key0")
    print(target_hostname)  # got node1hostname

    print(len(r))

    #add a 4th node to the ring

    #if the data is replicated on 2 other servers they would be the 
    #next two lar
    target_hostname = r.get_node_for_key("key6",2)
    print(target_hostname)  # got node1hostname

    target_hostname = r.get_node_for_key("key0",2)
    print(target_hostname)  # got node1hostname

    r.remove_node('node1')
    target_hostname = r.get_node_for_key("key0",2)
    print(target_hostname)  # got node3hostname

