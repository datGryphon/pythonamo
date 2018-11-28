import socket
import argparse

from node import Node

if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--leader', help='Hostname of leader')
    parser.add_argument('--port', default=13337, type=int, help='TCP port number for server socket')
    parser.add_argument('--qfrac', default=0.34, type=float, help='Fraction of peers on which data will be replicated')
    parser.add_argument('--sq_write_n', default=0.34, type=float, help='Min number of confirmed peers in a put operation with sloppy quorum')
    parser.add_argument('--sq_read_n', default=0.34, type=float, help='Number of polled peers in a get operation')

    args = parser.parse_args()

    is_leader = False  # is the current node the leader
    hostname = socket.gethostname()

    leader_hostname = args.leader
    if not leader_hostname or (leader_hostname == hostname):
        is_leader = True
        leader_hostname = hostname

    n = Node(is_leader, leader_hostname, hostname, tcp_port=args.port,
             sloppy_Qfrac=args.qfrac, sloppy_R=args.sq_read_n, sloppy_W=args.sq_write_n)

    n.accept_connections()
