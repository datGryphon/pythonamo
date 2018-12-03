import logging
import os
import socket
import struct
import select
import time
import json
from threading import Timer

import messages
from ring import Ring
from request import Request
from storage import Storage
from collections import defaultdict


class Node(object):

    def __init__(self, is_leader, leader_hostname, my_hostname, tcp_port=13337, sloppy_Qsize=5, sloppy_R=3, sloppy_W=3):

        self.ongoing_requests = []
        self.is_leader = is_leader
        self.leader_hostname = leader_hostname
        self.hostname = my_hostname
        self.tcp_port = tcp_port
        self.my_address = (self.hostname, self.tcp_port)

        self.membership_ring = Ring(replica_count=sloppy_Qsize - 1)  # Other nodes in the membership
        if self.is_leader:
            self.membership_ring.add_node(leader_hostname)

        # todo: look into this, do we need both?
        self.bootstrapping = True
        self.is_member = False

        self.sloppy_Qsize = sloppy_Qsize  # fraction of total members to replicate on
        # number of peers required for a read or write to succeed.
        self.sloppy_R = sloppy_R
        self.sloppy_W = sloppy_W

        # Book keeping for membership messages
        self._req_responses = defaultdict(set)
        self._sent_req_messages = {}
        self._req_sender = {}  # Keeps track to sender for add and delete requests
        self.current_view = 0  # increment this on every leader election
        self.membership_request_id = 0  # increment this on every request sent to peers

        self.log_prefix = '/code'
        self.ring_log_file = os.path.join(self.log_prefix, self.hostname + '.ring')
        self.db_path = os.path.join(self.log_prefix, self.hostname + '.db')
        print(self.ring_log_file)

        try:
            with open(self.ring_log_file, 'r') as f:
                hosts = f.readlines()
                for h in hosts:
                    self.membership_ring.add_node(h.strip())
            print("Restored from %s" % self.hostname + '.ring')
        except FileNotFoundError:
            pass

        self.request_timelimit = 2.0
        self.req_message_timers = {}

        self.db = Storage(self.db_path)  # set up sqlite table

        # create tcp socket for communication with peers and clients
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setblocking(False)  # Non-blocking socket
        self.tcp_socket.bind((self.hostname, self.tcp_port))
        self.tcp_socket.listen(10)

        # has hostnames mapped to open sockets
        self.connections = {}
        self.client_list = set()

        self.root = logging.getLogger()
        self.root.setLevel(logging.DEBUG)

    def accept_connections(self):
        incoming_connections = {self.tcp_socket}

        while True:
            readable, _, _ = select.select(incoming_connections, [], [], 0)
            for s in readable:
                if s is self.tcp_socket:
                    connection, client_address = s.accept()
                    connection.setblocking(False)

                    incoming_connections.add(connection)
                    self.connections[client_address[0]] = connection

                else:
                    try:
                        header = s.recv(5)
                    except:
                        print("Connection reset")
                        continue

                    if not header:  # remove for connection pool and close socket
                        incoming_connections.remove(s)
                        # del self.connections[s.getpeername()[0]]
                        s.close()
                    else:
                        message_len = struct.unpack('!i', header[1:5])[0]

                        data = b''
                        while len(data) < message_len:
                            try:
                                data += s.recv(message_len - len(data))
                            except socket.error as err:
                                pass

                        self._process_message(header + data, s.getpeername()[0])  # addr is a tuple of hostname and port
                        # # todo: is there a better way to find hostname?
                        # sender_hostname = self.dns[s.getpeername()[0]]
                        # self._process_message(header+data, sender_hostname)

    def _process_message(self, data, sender):
        message_type, data_tuple = messages._unpack_message(data)

        message_type_mapping = {
            b'\x00': self._process_command,
            b'\x01': self._process_req_message,
            b'\x10': self._process_new_view_message,
            b'\xff': self._process_ok_message,
            b'\x07': self.perform_operation,
            b'\x08': self.perform_operation,
            b'\x70': self.update_request,
            b'\x80': self.update_request,
            b'\x0B': self.update_request,
            b'\x0A': self.handle_forwarded_req,
            b'\x0C': self.handle_handoff
        }

        message_type_mapping[message_type](data_tuple, sender)
        return

    def _process_command(self, user_input, sendBackTo):
        """Process commands"""

        self.client_list.add(sendBackTo)

        # todo: if not self.leader, forward it to leader
        if not self.is_leader:
            print("Contact leader")
            return

        # Maps command to the corresponding function.
        # Command arguments are passed as the first argument to the function.
        command_registry = {  # Possible commands:
            "add-node": self.add_node,  # 1. add node to membership
            "remove-node": self.remove_node,  # 2. remove node from membership
            "put": self.put_data,  # 3. put data
            "get": self.get_data,  # 4. get data
            "delete": lambda x: x,  # 5. delete data
            "quit": lambda x: x  # 6. Quit
        }

        if not user_input:
            print("User input empty")
            return ""

        # First word is command. Rest are then arguments.
        command, *data = user_input.split(" ")
        if command not in command_registry:
            return "Invalid command"

        # Call the function associated with the command in command_registry
        return command_registry[command](data, sendBackTo)

    def handle_handoff(self,msg,sendBackTo):
        print("New Handoff message!!!",msg)

    def add_node(self, data, sender):
        """Add node to membership. data[0] must be the hostname. Initiates 2PC."""
        if not data:
            return "Error: hostname required"

        if data[0] in self.membership_ring:
            self._send_req_response_to_client(sender, "Already in the membership")
            return

        print("Sending req message to peers")
        print("Adding new node: %s : %s" % (self.current_view, self.membership_request_id))
        new_peer_message = messages.reqMessage(self.current_view, self.membership_request_id, 1, data[0])

        # associate hostname to (view_id, req_id)
        self._sent_req_messages[(self.current_view, self.membership_request_id)] = data[0]
        self._req_sender[(self.current_view, self.membership_request_id)] = sender

        # broadcast to all but leader.
        nodes_to_broadcast = self.membership_ring.get_all_hosts()
        nodes_to_broadcast.remove(self.hostname)
        nodes_to_broadcast.add(data[0])  # Add new host to broadcast list

        self.broadcast_message(nodes_to_broadcast, new_peer_message)

        # todo: handle timer
        t = Timer(self.request_timelimit, self._req_timeout, args=[(self.current_view, self.membership_request_id)])
        self.req_message_timers[(self.current_view, self.membership_request_id)] = t
        t.start()

        print("timer started")

        self.membership_request_id += 1

        return

    # Send a remove node message to everyone and if you are that node, shutdown
    def remove_node(self, data, sender):
        if not data:
            return "Error: hostname required"
        self.membership_ring.remove_node(data[0])

        return "removed " + data[0] + " from ring"

    def put_data(self, data, sendBackTo):
        if len(data) != 3:
            return "Error: Invalid operands\nInput: (<key>,<prev version>,<value>)"
        data = [data[0], json.loads(data[1]), data[2]]
        key = data[0]
        prev = data[1]
        value = data[2]
        target_node = self.membership_ring.get_node_for_key(data[0])
        if not self.is_leader:
            # forward request to leader for client
            return self._send_data_to_peer(self.leader_hostname, data, sendBackTo)
        else:  # I am the leader
            if target_node == self.hostname:
                # I'm processing a request for a client directly
                self.start_request('put', data, sendBackTo=sendBackTo)
                return "started put request for %s:%s locally [%s]" % (key, value, self.hostname)
            else:  # I am forwarding a request from the client to the correct node
                return self._send_data_to_peer(target_node, data, sendBackTo)

    def get_data(self, data, sendBackTo):
        """Retrieve V for given K from the database. data[0] must be the key"""
        if not data:
            return "Error: key required"
        target_node = self.membership_ring.get_node_for_key(data[0])
        # if I can do it myself
        if target_node == self.hostname:
            # I am processing a request for a client directly
            self.start_request('get', data[0], sendBackTo=sendBackTo)
            return "started get request for %s:%s locally [%s]" % (
                data[0], self.db.getFile(data[0]), self.hostname
            )
        else:  # forward the client request to the peer incharge of req
            return self._request_data_from_peer(target_node, data[0], sendBackTo)

    def _process_req_message(self, data, sender):
        # data = (view_id, req_id, operation, address)
        (view_id, req_id, operation, address) = data

        # todo: handle cases when not to send okay
        # # New member. Send okay
        # if not self.is_member:
        #     ok_message =

        print("Got req_message")
        ok_message = messages.okMessage(view_id, req_id)
        self.connections.get(sender, self._create_socket(sender)).sendall(ok_message)
        print("Sent okay to %s" % sender)

    def _process_ok_message(self, data, sender):
        print("got okay message")
        self._req_responses[data].add(sender)

        # number of replies equal number of *followers* already in the ring, add peer to membership
        if len(self._req_responses[data]) == len(self.membership_ring):

            # Cancel timer
            t = self.req_message_timers.get(data, None)
            if not t:
                print("No timer, what happened?")
                return

            print("Cancelling timer...")
            t.cancel()

            new_peer_hostname = self._sent_req_messages[data]
            self.membership_ring.add_node(new_peer_hostname)

            # Send newViewMessage
            self.current_view += 1
            new_view_message = messages.newView(self.current_view, self.membership_ring.get_all_hosts())

            nodes_to_broadcast = self.membership_ring.get_all_hosts()
            nodes_to_broadcast.remove(self.hostname)
            self.broadcast_message(nodes_to_broadcast, new_view_message)

            client = self._req_sender.get(data, None)
            print("sending success to client: %s" % client)
            self._send_req_response_to_client(client, "Successfully added node to the network")

        else:
            print("Only received okay from %d peers. Need %d confirmations" % (
                len(self._req_responses[data]), len(self.membership_ring)), self._req_responses[data])

    def _process_new_view_message(self, data, sender):
        (view_id, peers) = data
        self.current_view = view_id
        for p in peers:
            if p not in self.membership_ring:
                self.membership_ring.add_node(p)

        with open(self.ring_log_file, 'w') as f:
            for node in self.membership_ring.get_all_hosts():
                f.write(node + '\n')

        print("current members: ", self.membership_ring.get_all_hosts())

    def _req_timeout(self, req_id):
        print("request ", req_id, "timed out.")
        # todo: send failure
        sender = self._req_sender.get(req_id, None)
        self._send_req_response_to_client(sender, "Failed to add node to the network")

    def _send_req_response_to_client(self, client, message):
        msg = messages.responseForForward(message)
        self.broadcast_message([client], msg)

    # request format:
    # object which contains
    # type
    # sendBackTo
    # forwardedTo =None if type is not for_*
    # hash
    # value =None if type is get or forget
    # context =None if type is get or forget
    # responses = { sender:msg, sender2:msg2... }

    # args format is determined by type:
    #   type='get', args='hash'
    #   type='put', args=('hash','value',{context})
    #   type='for_get', args=(target_node,'hash')
    #   type='for_put', args=(target_node, 'hash','value',{context})
    # Type can be 'put', 'get', 'for_put', 'for_get'
    # 'for_*' if for requests that must be handled by a different peer
    # then when the response is returned, complete_request will send the
    # output to the correct client or peer (or stdin)
    def start_request(self, rtype, args, sendBackTo, prev_req=None):
        print("%s: New Request [ %s, %s ] for %s" % (
            self.hostname, rtype, args, sendBackTo
        ))
        req = Request(rtype, args, sendBackTo, previous_request=prev_req)  # create request obj
        self.ongoing_requests.append(req)  # set as ongoing

        target_node = self.membership_ring.get_node_for_key(req.hash)
        replica_nodes = self.membership_ring.get_replicas_for_key(req.hash)

        T = Timer(self.request_timelimit + (1 if rtype[:3] == 'for' else 0),
                  self.complete_request, args=[req], kwargs={"timer_expired":True}
                  )
        T.start()
        self.req_message_timers[req.time_created] = T

        # Find out if you can respond to this request
        if rtype == 'get':
            # add my information to the request
            result = self.db.getFile(args)
            my_resp = messages.getFileResponse(args, result, req.time_created)
            self.update_request(messages._unpack_message(my_resp)[1], socket.gethostbyname(self.hostname), req)
            # send the getFile message to everyone in the replication range
            msg = messages.getFile(req.hash, req.time_created)
            # this function will need to handle hinted handoff
            self.broadcast_message(replica_nodes, msg)
            print("Sent getFile message to %s" % replica_nodes)
        elif rtype == 'put':
            self.db.storeFile(args[0], socket.gethostbyname(self.hostname), args[1], args[2])
            my_resp = messages.storeFileResponse(args[0], args[1], args[2], req.time_created)
            # add my information to the request
            self.update_request(messages._unpack_message(my_resp)[1], socket.gethostbyname(self.hostname), req)
            # send the storeFile message to everyone in the replication range
            msg = messages.storeFile(req.hash, req.value, req.context, req.time_created)
            # this function will need to handle hinted handoff
            self.broadcast_message(replica_nodes, msg)
            print("Sent storeFile message to %s" % replica_nodes)
        else:
            msg = messages.forwardedReq(req)
            # forward message to target node
            # self.connections[req.forwardedTo].sendall(msg)
            if self.broadcast_message([req.forwardedTo], msg):
                self.leader_to_coord(req)
            else:
                print("Forwarded Request to %s" % req.forwardedTo)

        print("Number of ongoing Requests: ", len(self.ongoing_requests))

    def leader_to_coord(self,req):
        replica_nodes =  self.membership_ring.get_replicas_for_key(req.hash)
        req.type = req.type[4:]

        if req.type == 'get':
            msg = messages.getFile(req.hash, req.time_created)
        else:
            msg = messages.storeFile(req.hash, req.value, req.context, req.time_created)

        self.broadcast_message(replica_nodes, msg)
        print("Leader is assuming roll of coordinator")

    def find_req_for_msg(self, req_ts):
        return list(filter(
            lambda r: r.time_created == req_ts, self.ongoing_requests
        ))

    # after a \x70, \x80 or \x0B is encountered from a peer, this method is called
    def update_request(self, msg, sender, request=None):
        print("Updating Request with message ", msg, " from ", sender)
        if isinstance(msg, tuple):
            if not request:
                request = self.find_req_for_msg(msg[-1])
            min_num_resp = self.sloppy_R if len(msg) == 3 else self.sloppy_W
        else:
            request = self.find_req_for_msg(msg.previous_request.time_created)
            min_num_resp = 1

        if not request:
            print("No request found, ", sender, " might have been too slow")
            return
        elif isinstance(request, list):
            request = request[0]

        request.responses[sender] = msg
        if len(request.responses) >= min_num_resp:
            self.complete_request(request)

    def coalesce_responses(self, request):
        resp_list = list(request.responses.values())
        # check if you got a sufficient number of responses
        if len(resp_list) < self.sloppy_R:
            return None
        results = []
        for resp in resp_list:
            # print(resp)
            results.extend([
                tup for tup in resp[1] if tup not in results
            ])
        return self.db.sortData(results)

    def complete_request(self, request, timer_expired=False):

        # self.req_message_timers[request.time_created].cancel()
        # (peername,msg) tuples for hinted handoff
        handoffs = []

        failed = False

        print("Completed Request ")
        if request.type == 'get':
            # if sendbackto is a peer
            print("Response for client (name: ", request.hash, ", results: ", self.coalesce_responses(request), ") ")
            if request.sendBackTo not in self.client_list:
                # this is a response to a for_*
                # send the whole request object back to the peer
                msg = messages.responseForForward(request)
            else:
                # compile results from responses and send them to client
                # send message to client
                msg = messages.getResponse(request.hash, (
                    self.coalesce_responses(request) if not failed else "Error"
                ))
        elif request.type == 'put':
            if len(request.responses) >= self.sloppy_W:
                print("Sucessful put completed for ", request.sendBackTo)

            if request.sendBackTo not in self.client_list:
                # this is a response to a for_*
                # send the whole request object back to the peer
                msg = messages.responseForForward(request)
            else:
                # send success message to client
                # check if you were successful
                msg = messages.putResponse(request.hash, (
                    request.value if len(request.responses) >= self.sloppy_W and not failed else "Error"
                ), request.context)
            
            if len(request.responses) >= self.sloppy_W and timer_expired:
                target_node = self.membership_ring.get_node_for_key(request.hash)
                replica_nodes = self.membership_ring.get_replicas_for_key(request.hash)
                
                all_nodes = set([target_node]+replica_nodes)
                missing_reps = set([ socket.gethostbyname(r) for r in all_nodes]) - set(request.responses.keys())

                print(all_nodes, missing_reps, set(request.responses.keys()))

                handoff_msg = messages.handoff(
                    messages.storeFile(request.hash,request.value,request.context,request.time_created),
                    missing_reps
                )

                hons = [ 
                    self.membership_ring.get_handoff_node(r)
                    for r in missing_reps
                ]
                
                self.broadcast_message(hons,handoff_msg)

        else:  # request.type == for_*
            # unpack the forwarded request object
            data = list(request.responses.values())

            if not data:
                # del self.req_message_timers[request.time_created]
                # request.time_created=time.time()
                self.leader_to_coord(request)
                T = Timer(self.request_timelimit,
                  self.complete_request, args=[request], kwargs={"timer_expired":True}
                  )
                T.start()
                self.req_message_timers[request.time_created] = T
                return
            else:
                data = data[0]
                print("Request Response Data: ", data)
                # if sendbackto is a peer
                if request.sendBackTo not in self.client_list:
                    # unpickle the returned put request
                    data.previous_request = data.previous_request.previous_request
                    # send the response object you got back to the peer
                    # from request.responses (it is the put or get they need)
                    # if you need to, make req.prev_req = req.prev_req.prev_req
                    # so it looks like you did the request yourself
                    msg = messages.responseForForward(data)
                elif request.type == 'for_put':
                    msg = messages.putResponse(request.hash, (
                        request.value if data and len(data.responses) >= self.sloppy_W and not failed else "Error"
                    ), request.context)
                else:  # for_get
                    print("Response for client (name: ", request.hash, ", results: ", self.coalesce_responses(data),
                          ") ")
                    msg = messages.getResponse(request.hash, (
                        self.coalesce_responses(data) if not failed else "Error"
                    ))

        # send msg to request.sendBackTo
        # if request.sendBackTo not in self.client_list:
        if not request.responded:
            self.broadcast_message([request.sendBackTo], msg)
            request.responded=True

            print(msg)
            # self.connections[request.sendBackTo].sendall(msg)

            print("Sent results back to ", request.sendBackTo)

        if timer_expired:
            # remove request from ongoing list
            self.ongoing_requests = list(filter(
                lambda r: r.time_created != request.time_created, self.ongoing_requests
            ))
            print("Number of ongoing Requests: ", len(self.ongoing_requests))

    def perform_operation(self, data, sendBackTo):
        if len(data) == 2:  # this is a getFile msg
            print(sendBackTo, " is asking me to get ", data)
            msg = messages.getFileResponse(data[0], self.db.getFile(data[0]), data[1])
        else:  # this is a storeFile
            if data[0][:5] == 'sleep':
                print(data)
                time.sleep(int(data[2]))
            print(sendBackTo, " is asking me to store", data)
            self.db.storeFile(data[0], sendBackTo, data[1], data[2])
            msg = messages.storeFileResponse(*data)

        self.broadcast_message([sendBackTo], msg)
        # self.connections[sendBackTo].sendall(msg)
        print("Completed operation for ", sendBackTo)

    def handle_forwarded_req(self, prev_req, sendBackTo):
        target_node = self.membership_ring.get_node_for_key(prev_req.hash)
        print("Handling a forwarded request [ %s, %f ]" % (prev_req.type, prev_req.time_created))
        
        if time.time() - prev_req.time_created < self.request_timelimit:
            # someone forwarded you a put request
            # if you are the leader, check if you can takecare of it, else,
            # start a new put request with this request as the previous one
            if prev_req.type == 'put' or prev_req.type == 'for_put':
                if self.is_leader:
                    if target_node == self.hostname:
                        args = (prev_req.hash, prev_req.value, prev_req.context)
                        self.start_request('put', args, sendBackTo, prev_req=prev_req)
                        print("handling forwarded put request locally")
                    else:
                        args = (target_node, prev_req.hash,
                                prev_req.value, prev_req.context
                                )
                        self.start_request('for_put', args, sendBackTo, prev_req)
                        print("Forwarded Forwarded put request to correct node")
                else:  # the leader is forwarding you a put
                    args = (prev_req.hash, prev_req.value, prev_req.context)
                    self.start_request('put', args, sendBackTo, prev_req=prev_req)
                    print("handling forwarded put request locally")
            # someone forwarded you a get request, you need to take care of it
            # start new get request with this as the previous one
            else:  # type is get or for_get
                self.start_request('get', prev_req.hash, sendBackTo, prev_req)
                print("handling forwarded get request locally")
        else:
            print("Throwing away stale request.")

    def _send_data_to_peer(self, target_node, data, sendBackTo):

        # create for_put request
        self.start_request('for_put', [target_node] + data, sendBackTo=sendBackTo)

        return "forwarded put request with %s to node %s" % (data, target_node)

    def _request_data_from_peer(self, target_node, data, sendBackTo):

        self.start_request('for_get', (target_node, data), sendBackTo=sendBackTo)

        return "requesting data from %s" % target_node

    def _create_socket(self, hostname):
        """Creates a socket to the host and adds it connections dict. Returns created socket object."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(10)  # 10 seconds
        try:
            s.connect((hostname, self.tcp_port))
            self.connections[socket.gethostbyname(hostname)] = s
            return s
        except Exception as e:
            print(e)
            print("Error creating connection to %s. Probably down?" % hostname)
            return None

    # this is where we need to handle hinted handoff if a
    # peer is not responsive by asking another peer to hold the
    # message until the correct node recovers
    def broadcast_message(self, nodes, msg):
        fails = []
        for node in nodes:
            c = self.connections.get(node, self._create_socket(node))
            if not c:
                fails.append(node)
                continue
            c.sendall(msg)

        return fails
