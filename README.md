# python-dynamo

Protocol for building the ring

All hosts have the same code

Start the first host and don't provide a boot-strap id
	
This is the first node in the ring

For each node thereafter, provide the address of any node in the ring

New peer connects to that host and sends a newPeer request

Peer sends a request to all peers asking if a new node can join
	if all nodes say yes, then the peer sends back a list of all peers
	to the new peer. 
		The new peer connects to all the peers
		When the new peer has received all data from all necessary peers
			and is connected to all of them, he sends the all clear message to all peers
			If a peer cannot connect to any peer it sends an abort message
				and terminates the connections
		Upon receiving the all clear message existing peers will start actually using the new peer and will add it to its membership ring.

	If any node says no, because it is currently in the process of adding a peer
		enter a slightly randomized exponential backoff trying to get permission to add a peer. 

Removing nodes from membership

Nodes must be removed from membership via a client command

When a node gets the command, it broadcasts to all nodes,
	If a node is not responding, put this reminder on a timeout

Put method

Client sends a request to a server to store an object,

They must provide the hash and binary data for the object as
	well as the previous context

If the peer receiving the request is not the node in the replication range for that object (referred to as the preference list in the paper) then the request is forwarded to the first node in that list that is responsive

If no node in the preference list is responsive, return an error to the client

When a node in the preference list receives the request, it stores the value, and sends out a storeFile request to all peers in the replication range 
When it receives a response from the min number of replicas, it returns success to the client or the peer that sent it the request.


Get method

Client sends a request to a server to retrieve an object

The peer sends a getFile request to all peers in the replica range
	when he receives the min number of responses, he combines
	the unique elements and sorts them by vector clock, then sends
	the pickled list back to the client.