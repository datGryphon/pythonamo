README--------------------------CALEB-WASTLER---------------------------PROJECT3

								   APPROACH

	The highlevel approach that I adopted for this project was to create a non-
blocking, single threaded application for each of the 3 components (root, peer, 
and client).  To store and manage files, each node in the ring maintains a 
simple two-entry sqlite3 table which stores a hash and a binary blob every row.
The general description of the ring is best presented as follows:

	1) The root server which is started first, binds its address and then goes 
		into its main functional loop which handles 4 main situations
		a) accepting new inbound connections
		b) correctly bootstraping new peers and new clients to the network from 
			a pool of not-yet identified connections
		c) handle initial requests from all clients which can include storing
			files, returning files, and sending back the correct location to 
			perform a specific action after querying the rest of the ring
		d) if possible, repair the connection between the root server and the 
			rest of the ring if the roots immediate child drops out
	2) The peer servers which start by connecting to the root server to ask for
		its position in the ring. After this the new peer is responsible for:
		a) Connecting to its new parent and connecting to its new child (if any)
			and receiving all files from the parent that the new peer is now 
			responsible for
		b) Listening for any 'downstream' requests from its parent and either, 
			responding (e.g. with its address so that a peer can contact this
			peer to store or retreive files) or forwarding the message
			downstream to its child
		c) Listening for any 'upstream' responses and forwarding them along the 
			parent
		d) Listening for any inbound client connections in the event that the 
			client is performing a iterative search
		e) if possible, repair the connection between the peer and the 
			rest of the ring if the peer's immediate child or parent drops out
	3) The client application which connects to the root server, and announces 
		that it is a client so it is put into the client pool, then:
		a) store a file located locally
		b) retrieve a file from the ring either by recursive search  (root does 
			the search) or iterative search (client connects to each peer), when
			a files is retreived, it is displayed on the console as well as 
			written to disk with the original filename followed by the PID of
			the client process which performed the lookup.
		c) send a command to the root server to force the ring to print its 
			current state
		d) send a command to the root server to force the ring to shut down
		e) gracefully close the connection to the root server when finished

								   EXECUTE

	To run this project, there are only two steps. 

	1) use make to convert the python files in the project folder to executables

		$make dht_peer
		$make dht_client

	2) On different CCIS machines, instantiate one part of the program

		Root:
		$./dht_peer -m 1 -h host.ccs.neu.edu -p ####

		Peer:
		$./dht_peer -m 0 -h host.ccs.neu.edu -p #### -r root.ccs.neu.edu -R ####

		Client:
		$./dht_client -h client.ccs.neu.edu -p #### -r root.ccs.neu.edu -R ####

								   TESTING

	The way I tested my code was in an incremental fashion. First, I made sure 
that everything worked correctly when the root was the only node in the ring
including support for multiple clients to have an open connection with the ring. 
This way I could more easily isolate what was going on later. Then, I started to 
test the bootstrapping process for peer nodes.  For testing this I used 7 peers 
in addition to my root server. First, I made sure that I could support 3 nodes
in my ring (including the root) because these were the ones that were 
immediately visible from the root node which meant that the root was entirely 
responsible for bootstrapping them and making sure they successfully attached
themselves to the ring. Then I tested the bootstrap process for the parent peers
to successfully insert a child after themselves in the ring (which also 
included coordinating with its old child). After I had extensively tested the 
different cases of my bootstrap process (i.e. has grandparent or not, does or 
does not have a child/grandchild, does or does not have files) I moved on to 
testing the store and recursive file functionality with a full ring. After, I 
wrote and began testing the functionality to repair the ring structure when a
peer drops out. Again, I set out to try and test the majority of cases for the 
state of the ring before a peer drops out and where a peer drops out. Finally, 
I added in the functionality at the end to perform an iterative search. Which 
was very simple and easy to patch in once the rest of the ring was working.

The testing enviroment I used is as follows:

	Root Server: sombrero.ccs.neu.edu

	Peer Servers (in the order they fit into the final ring):
		nightcap.ccs.neu.edu
		bere.ccs.neu.edu
		top.ccs.neu.edu
		mortarboard.ccs.neu.edu
		baseball.ccs.neu.edu
		pilotka.ccs.neu.edu
		skullcap.ccs.neu.edu

	Client Server: captain.ccs.neu.edu

	Note: To greatly simplify repairing the ring, the program relies on the fact
		that all of the peers, including the root are passed the same port with
		the -p tag when the ring is instantiated. In testing, I consistently 
		used the port 15151.

								  CHALLENGES

	I think the most difficult part of this project was the boot strapping 
process. Figuring out an effective way to insert a new peer between two nodes, 
and make sure that he got all of the correct files was a very large and 
challenging problem to think about. After I had finalized that process, 
the rest of the design choices kind-of fell into place and the speed of 
development rose significantly. 
								     
								     BUGS

	There are a handful of know problems with my program. The first is that to
make sure each peer successfully connects to the ring, wait to confirm that the
first peer has entered its main loop before starting the second peer's program.
The second is that sometimes, due to network or processing speeds, when a client
tries to connect to one of the peers in the ring, it will time out and go back
to the main loop without actually performing the store/lookup. Also, in the rare
coincidence that two nodes are started with the same PID on different machines,
the second peer will fail because it cannot create the directory in which 
it stores its sqlite table. This is because the directory that each peer creates
is followed by its PID.
