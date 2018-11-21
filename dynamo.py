import socket
import argparse

from node import Node

if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--leader', help='Hostname of leader')
    parser.add_argument('--port', default=13337, type=int, help='TCP port number')
    parser.add_argument('--qfrac', default=0.34, type=float, help='TCP port number')

    args = parser.parse_args()

    is_leader = False  # is the current node the leader
    hostname = socket.gethostname()

    leader_hostname = args.leader
    if not leader_hostname or (leader_hostname == hostname):
        is_leader = True
        leader_hostname = hostname

    n = Node(is_leader, leader_hostname, hostname, tcp_port=args.port, sloppy_Qfrac=args.qfrac)
    n.accept_connections()
