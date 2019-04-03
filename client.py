#!/usr/bin/env python3

import socket
from xml.dom import minidom
import sys
HOST = '0.0.0.0'  # The server's hostname or IP address
PORT = 12345        # The port used by the server
PORT_IN = 23456
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

f=open(sys.argv[1], "r")
if f.mode == 'r':
    contents =f.read()

class Connection:
        def send_to_server(self):
          
            s.connect((HOST, PORT))
            #s.sendall(b'<create><symbol sym="AAA"><account id="666">100</account></symbol></create>')
            s.sendall(bytes(contents, 'utf-8'))
            #data = s.recv(1024)
            #s.sendall(b'<transactions id="10"><order sym="BBB" amount="1000" limit="100"/></transactions>')
        def receive(self):
            xml = ''
          
            data = s.recv(10240)
          
            xml += str(data,'utf-8')
                #conn.sendall(data)
            print(minidom.parseString(xml).toprettyxml(indent = "    "))

def main():
    con = Connection()
    con.send_to_server()
    con.receive()


if __name__ == "__main__":
    main()
