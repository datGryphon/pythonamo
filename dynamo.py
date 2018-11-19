import socket
import argparse

from node import Node

if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--leader', help='Hostname of leader')

    args = parser.parse_args()

    is_leader = False  # is the current node the leader
    hostname = socket.gethostname()

    leader_hostname = args.leader
    if not leader_hostname or (leader_hostname == hostname):
        is_leader = True
        leader_hostname = hostname

        n = Node(is_leader, leader_hostname)

        #enter main loop
        while True:
            user_input = input("++> ")
            result = n._process_command(user_input)

            print(result)

    #else not the leader

    #enter join loop

        #enter main loop
