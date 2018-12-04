# pythonamo

DATABASE PROGRAM:
=================
To start the database node, use the program dynamo.py.
The leader should have its own hostname as the argument for --leader.
The peers should have leader's hostname as the argument for --leader.

qsize is the number of nodes the data will be replicated on.
write_n is the minimum number of confirmed writes for a successful PUT operation.
read_n is the minimum number of confirmed reades for a successful GET operation.

Program: dynamo.py
Arguments: --leader LEADER
           --port PORT
           --qsize QSIZE
           --sq_write_n SQ_WRITE_N
           --sq_read_n SQ_READ_N

CLIENT PROGRAM:
===============
To start a client to interact with the database, use the program client.py.
The program expects a dynamo node hostname and port for the --node  and --port arguments. 

Program: client.py
Arguments: --node NODE HOSTNAME
           --port PORT


DATABASE CLIENT COMMANDS:
=========================

1. add-node <hostname>
   Use this command to add an additional node to the membership ring.
   This command can only be executed by the leader. Leader expects the new node to be
   running already. 

2. remove-node <hostname>
   Use this command to remove a node from the membership ring.
   This command can only be executed by the leader.

3. put <key> <context> <value>
   Use this command to store a key value pair in the database.
   Key and value are expected to be strings without spaces in them.
   Context is a JSON object passed in a string without spaces. 
   Example for context: {"192.168.1.69":2,"192.168.1.70":2}
   A empty context can be passed by {}  

4. get <key>
   Use this command to retreive a value given the key.
   

DOCKER:
=======
This project has been developed and tested on Docker. For the ease of use, a docker-compose file has been
provied along with make directives.

To install docker-compose, use `pip install docker-compose`.

To start the database nodes: make run-db-docker
To start a client connected to the leader: make run-client-docker
To auto insert nodes into the ring and start a client: make testcase
To clean all the persistent files: make clean
To stop all running containers: make stop-docker
To start any individual node from the docker-compose.yaml: docker-compose up <machine number>
To start any individual client from the docker-compose.yaml: docker-compose run client1/client2

To run nodes on the local machine instead of docker: python3 -u dynamo.py --leader <leader hostname> --port <port>
 



