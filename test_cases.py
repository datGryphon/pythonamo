import argparse
import socket
import sys
import time

import messages

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--leader', help='Hostname of leader')
parser.add_argument('--port', help='TCP port', type=int)

args = parser.parse_args()

try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((args.leader, args.port))
except Exception as e:
    print("Cannot start client. Reason: %s" % e)
    sys.exit(1)

nodes = [
    "vdi-linux-031.ccs.neu.edu",
    "vdi-linux-032.ccs.neu.edu",
    "vdi-linux-033.ccs.neu.edu",
    "vdi-linux-034.ccs.neu.edu",
    "vdi-linux-035.ccs.neu.edu",
    "vdi-linux-036.ccs.neu.edu",
    "vdi-linux-037.ccs.neu.edu",
    # "vdi-linux-038.ccs.neu.edu",
    # "vdi-linux-039.ccs.neu.edu",
]


def add_nodes():
    for node in nodes:
        msg = messages.client_message("add-node %s" % node)
        s.sendall(msg)

        time.sleep(3)


if __name__ == '__main__':
    add_nodes()
