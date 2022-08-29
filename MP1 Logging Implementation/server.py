#server.py

import socket
import json

HOST = "127.0.0.1"
PORT = 8080
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print(f"Connected by {addr}")
        try:
            data = conn.recv(1024)
            pattern=json.loads(data.decode('utf-8'))["pattern"]
            print(f"Received Pattern: {pattern}")
            conn.sendall(json.dumps(pattern).encode('utf-8'))
            print("Sent Data...")
        except Exception as e:
            print(e)