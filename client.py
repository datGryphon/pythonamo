import argparse
import socket
import sys

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

while True:
    user_input = input("++> ")
    if not user_input:
        continue

    msg = messages.client_message(user_input)
    s.sendall(msg)
