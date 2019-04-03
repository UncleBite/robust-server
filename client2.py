#!/usr/bin/env python3

import socket
from xml.dom import minidom

HOST = '0.0.0.0'  # The server's hostname or IP address
PORT = 12345        # The port used by the server
PORT_IN = 23456
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
class Connection:
        def send_to_server(self):
            print('sending')
            s.connect((HOST, PORT))
            #s.sendall(b'<create><symbol sym="AAA"><account id="666">100</account></symbol></create>')
            s.sendall(b'<transactions id="2"><order sym="AAA" amount="1" limit="103"/></transactions>')
            #data = s.recv(1024)
            #s.sendall(b'<transactions id="10"><order sym="BBB" amount="1000" limit="100"/></transactions>')
        def receive(self):
            xml = ''
            print('receiving')
            data = s.recv(10240)
            print(data)
            xml += str(data,'utf-8')
                #conn.sendall(data)
            print(minidom.parseString(xml).toprettyxml(indent = "    "))

def main():
    con = Connection()
    con.send_to_server()
    con.receive()


if __name__ == "__main__":
    main()
