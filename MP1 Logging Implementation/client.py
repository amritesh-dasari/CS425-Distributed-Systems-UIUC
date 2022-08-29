# Client.py
import time
import sys
import threading
import socket
import json

# VMS = []
VMS="127.0.0.1"
PORT = 8080

# with open('serverslist.txt','r') as serverslist:
#     # Reading Servers List from the text file
#     lines = serverslist.readlines()
#     serverslist.close()
#     for line in lines:
#         serverip = line.split("\n")[0]
#         VMS.append(serverip)

def querythreads(host, port, pattern):
    # WIP testing individual threads
    resultlogs={}
    queryvar=json.dumps({"pattern":pattern}).encode('utf-8')
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_connection:
        server_connection.connect((host, port))
        server_connection.sendall(queryvar)
        result=""
        while True:
            resdata=server_connection.recv(1024)
            if resdata:
                result+=json.loads(resdata.decode('utf-8'))
            else:
                if result:
                    print(result)
                    resultlogs[host]=result
                break

    print(host, port, pattern)

def search_query(pattern):
    # Search Query method to maintain multiple Threads
    startwatch = time.time()
    threads=[]

    # Testing
    threads.append(threading.Thread(target=querythreads, args=(VMS,PORT,pattern)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    
    # for vm in VMS:
    #     threads.append(threading.Thread(target=querythreads, args=(vm,PORT,pattern)))
    # for thread in threads:
    #     thread.start()
    # for thread in threads:
    #     thread.join()
    stopwatch=time.time()
    print("Stopwatch Time : ",stopwatch-startwatch)

if __name__ == '__main__':
    pattern=sys.argv[1]
    print(f"Executing {sys.argv[0]} with pattern {pattern}")
    search_query(pattern)