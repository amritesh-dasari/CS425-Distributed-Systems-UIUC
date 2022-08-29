# Client.py
import time
import sys
import threading

VMS = []
PORT = 8080

with open('serverslist.txt','r') as serverslist:
    # Reading Servers List from the text file
    lines = serverslist.readlines()
    serverslist.close()
    for line in lines:
        serverip = line.split("\n")[0]
        VMS.append(serverip)

def querythreads(host, port, pattern):
    # WIP testing individual threads
    print(host, port, pattern)

def search_query(pattern):
    # Search Query method to maintain multiple Threads
    startwatch = time.time()
    threads=[]
    for vm in VMS:
        threads.append(threading.Thread(target=querythreads, args=(vm,PORT,pattern)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stopwatch=time.time()
    print("Stopwatch Time : ",stopwatch-startwatch)

if __name__ == '__main__':
    pattern=sys.argv[1]
    print(sys.argv)
    search_query(pattern)