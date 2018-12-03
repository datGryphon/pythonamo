import argparse
import socket
import select
import sys
import struct
import messages

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--leader', help='Hostname of leader')
parser.add_argument('--port', help='TCP port', type=int)

args = parser.parse_args()

try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((args.leader, args.port))

    # create tcp socket for communication with peers and clients
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.bind(("", args.port))
    tcp_socket.listen(10)
except Exception as e:
    print("Cannot start client. Reason: %s" % e)
    sys.exit(1)
else:
    user_input = input("++> ")
    if not user_input:
        exit(0)

    msg = messages.client_message(user_input)
    s.sendall(msg)

    socks = [sys.stdin.fileno(), tcp_socket, s]

while True:

    print('++>', end='')
    sys.stdout.flush()

    readable = select.select(socks, [], [])[0]

    for fd in readable:
        if fd is sys.stdin.fileno():
            msg = input()
            if msg:
                s.sendall(messages.client_message(msg))
            else:
                s.close()
                exit(0)
        elif fd is tcp_socket:
            conn, addr = tcp_socket.accept()
            socks.append(conn)
        else:
            msg = fd.recv(5)

            if not msg:
                fd.close()
                socks = [f for f in socks if f is not fd]
            else:
                message_len = struct.unpack('!i', msg[1:5])[0]

                data = b''
                while len(data) < message_len:
                    data += fd.recv(message_len - len(data))

                print(messages._unpack_message(msg + data)[1])
