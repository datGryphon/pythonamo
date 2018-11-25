import socket
import argparse

from node import Node

if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--peer', help='Hostname of a known peer in the system')
    parser.add_argument('--port', default=13337, type=int, help='TCP port number for server socket')
    parser.add_argument('--qfrac', default=0.34, type=float, help='Fraction of peers on which data will be replicated')
    parser.add_argument('--sq_write_n', default=0.34, type=float, help='Min number of confirmed peers in a put operation with sloppy quorum')
    parser.add_argument('--sq_read_n', default=0.34, type=float, help='Number of polled peers in a get operation')

    args = parser.parse_args()

    is_leader = False  # is the current node the leader
    hostname = socket.gethostname()

    peer_hostname = args.peer
    if not peer_hostname or (peer_hostname == hostname):
        is_leader = True
        peer_hostname = hostname

    n = Node(is_leader, leader_hostname, hostname, tcp_port=args.port, 
            sloppy_Qfrac=args.qfrac, sloppy_R=args.sq_read_n, sloppy_W=args.sq_write_n)
    
    n.start()