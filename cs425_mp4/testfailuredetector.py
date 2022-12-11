import socket
import random
from datetime import datetime
from collections import OrderedDict
import threading
import json
import time

INTRODUCER = "fa22-cs425-7301.cs.illinois.edu"

class FailureDetector:
    def __init__(self):
        self.host = socket.gethostname()
        self.id = str(random.randint(0, 4096)) + "_" + self.host + "_"+ datetime.now().strftime('%H:%M:%S')
        self.port = 8080
        self.introducer = True if (self.host==INTRODUCER) else False
        self.membershiplist = OrderedDict()
        self.membershiplist[self.host]={'id': self.id, 'ts': datetime.now().strftime('%H:%M:%S'), 'status': "INACTIVE"}
        self.neighbours = []
        self.distance = []
        self.membership_lock = threading.Lock()
        self.timer_lock = threading.Lock()
    
    def run(self):
        listener_thread = threading.Thread(target=self.listener)
        pinger_thread = threading.Thread(target=self.pinger)
        timer_thread = threading.Thread(target=self.timercheck)
        command_thread = threading.Thread(target=self.command_func)

        listener_thread.start()
        pinger_thread.start()
        timer_thread.start()
        command_thread.start()
        
        listener_thread.join()
        pinger_thread.join()
        timer_thread.join()
        command_thread.join()
    
    def listener(self):
        membershiplist = self.membershiplist
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection_listener:
            connection_listener.bind((self.host, self.port))
            while True:
                try:
                    if membershiplist[self.host]['status'] == "INACTIVE" or membershiplist[self.host]['status'] == "ERRORED":
                        # print(f"Listener offline because current status is {membershiplist[self.host]['status']}")
                        continue
                    packet, peer = connection_listener.recvfrom(1024)
                    if packet:
                        print(f"packet receivd from {peer}")
                        data = json.loads(packet.decode('utf-8'))
                        print(f"data: {data}")
                        packet_type = data.get('message_type', 'NA')
                        print(f"packet_type: {packet_type}")

                        if packet_type == "NA":
                            log = f"[ERROR]: Ignoring UDP packet from {peer}:{data['port']} containing no message type"
                            print(log)
                            # self.logsaver(logs=log, packet=data)
                            continue

                        sender_host = data.get('host')
                        print(f"sender_host: {sender_host}")
                        listener_timestamp = datetime.now().strftime('%H:%M:%S')
                        self.membership_lock.acquire()

                        if packet_type == "JOIN":
                            log = f"[INFO]: Received JOIN packet from {peer} for {sender_host}"
                            print(log)
                            # self.logsaver(logs = log, packet = data)
                            membershiplist[sender_host] = data.get('membershiplist')
                            membershiplist[sender_host]['status'] = "ACTIVE" 
                            membershiplist[sender_host]['timestamp'] = listener_timestamp
                            log = f"[INFO]: Successfully updated Membership List"
                            print(log)
                            # self.logsaver(logs = log)

                            if self.introducer is True:
                                print("Sending join packets to everyone as INTRODUCER")
                                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as temp_connection:
                                    temp_connection.bind((self.host,8082))
                                    packet = {
                                        'membershiplist': membershiplist[sender_host],
                                        'host':sender_host,
                                        'port':'8080', 
                                        'message_type': "JOIN"
                                    }
                                    for host in membershiplist.keys():
                                        if host != self.host:
                                            log = f"[INFO]: Sending JOIN packet from {sender_host} to {host}"
                                            print(log)
                                            # self.logsaver(logs = log, packet = packet)
                                            temp_connection.sendto(json.dumps(packet).encode('utf-8'), (host, 8080))    
                            self.update_neighbours()

                        elif packet_type == "LEAVE":
                            log = f"[INFO]: Received LEAVE packet from {sender_host}"
                            print(log)
                            # self.logsaver(logs=log, packet=data)
                            membershiplist[sender_host]['status'] = "INACTIVE"
                            membershiplist[sender_host]['timestamp'] = listener_timestamp

                        self.membership_lock.release()
                except Exception as e:
                    log = f"[ERROR]: Exception at Listener {str(e)}"
                    print(log)
                    # self.logsaver(logs=log)
                    continue

    def pinger(self):
        pass

    def timercheck(self):
        pass

    def join(self):
        membershiplist = self.membershiplist
        membershiplist[self.host]['timestamp'] = datetime.now().strftime('%H:%M:%S')
        print(f"Updated Current status to JOINING")
        membershiplist[self.host]['status'] = "JOINING"

        if self.introducer is False:
            print(f"{self.host} is not the introducer!")
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
                connection.bind((self.host,8081))
                packet = {
                    'membershiplist': membershiplist[self.host],
                    'host':self.host,
                    'port':self.port, 
                    'message_type': "JOIN"
                }
                log = f"[INFO]: Sending JOIN packet to INTRODUCER: {INTRODUCER}"
                print(log)
                # self.logsaver(logs=log, packet=packet)
                connection.sendto(json.dumps(packet).encode('utf-8'), (INTRODUCER, 8080))
                print("JOIN packet sent to introducer")
        else:
            print("I am the introducer")
            membershiplist[self.host]['status'] = "ACTIVE"

    def leave(self):
        membershiplist = self.membershiplist
        if membershiplist[self.host]['status'] != 'ACTIVE':
            log = f"[ERROR]: Cannot change status from {membershiplist[self.host]['status']} to 'INACTIVE' "
            print(log)
            # self.logsaver(logs=log)
            return
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
            connection.bind((self.host,8083))
            packet = {
                'port': self.port,
                'host': self.host,
                'message_type': 'LEAVE'
            }
            for neighbour in self.neighbours:
                log = f"[INFO]: Sending LEAVE packet to {neighbour}"
                print(log)
                # self.logsaver(logs=log,packet=packet)
                connection.sendto(json.dumps(packet).encode('utf-8'),(neighbour, 8080))
            membershiplist[self.host]['status'] = "INACTIVE"
        log = f"[INFO]: Successfully left the network"
        print(log)
        # self.logsaver(logs=log)
    
    def update_neighbours(self):
        membershiplist = self.membershiplist
        print(f"membershiplist: {membershiplist}")
        neighbours = self.neighbours
        print(f"neighbours: {neighbours}")
        distance = self.distance
        print(f"distance: {distance}")
        hostnum = int(self.host.split("-")[2][2:4])
        
        if len(neighbours)>0:
            print("Checking for inactive or errored nodes")
            for index in range(len(neighbours)):
                if membershiplist[neighbours[index]]['status']!="ACTIVE":
                    print(f"Removing {neighbours[index]} from neighbours list, status {membershiplist[neighbours[index]]['status']}")
                    del neighbours[index]
                    del distance[index]
        
        if len(neighbours)<4:
            print("Number of neighbours is less than 4")
            for node in membershiplist.keys():
                if (node != self.host and membershiplist[node]['status'] == "ACTIVE" and node not in neighbours):
                    print(f"Adding node:{node}")
                    neighbours.append(node)
                    nodenum = int(node.split("-")[2][2:4])
                    distance.append(min(abs(hostnum-nodenum),10-abs(hostnum-nodenum)))
                    if len(neighbours)==4:
                        break
        
        for node in membershiplist.keys():
            if (node!=self.host and membershiplist[node]['status']=="ACTIVE" and node not in neighbours):
                print(f"Current node: {node}")
                tempnodenum = int(node.split("-")[2][2:4])
                tempdistance = min(abs(hostnum-tempnodenum), 10-abs((hostnum-tempnodenum)))
                print(f"Node distance from host: {tempdistance}")
                currentmaxdistance = max(distance)
                print(f"Farthest node distance in the neighbours list: {currentmaxdistance}")
                if tempdistance>currentmaxdistance:
                    continue
                else:
                    for index in range(len(distance)):
                        if tempdistance<distance[index]:
                            print(f"replacing {neighbours[index]} with {node}")
                            del neighbours[index]
                            neighbours.append(node)
                            del distance[index]
                            distance.append(tempdistance)
                            break


    
    def logsaver(self, logs, packet):
        pass

    def command_func(self):
        while True:
            print('''
            1) join
            2) timer
            3) leave
            4) list
            5) grep
            6) details
            7) neighbours
            ''')
            arg = input('Enter Command Argument: ')
            
            if arg == 'join':
                self.join()
            # elif arg == 'timer':
            #     self.print_timer_list()
            # elif arg =='self':
            #     self.list_self()
            elif arg == 'leave':
                self.leave()
            elif arg == 'list':
                self.print_membership_list()
            # elif arg == 'grep':
            #     phrase=input('Enter GREP phrase (Press Enter to do cat): ')
            #     self.grep(phrase)
            # elif arg == 'details':
            #     # self.logsaver(logs="[INFO]: Request for server details received", packet={"id":self.id, "host":self.host, "port":self.port, "introducer":self.introducer})
            #     print(f"ID         : {self.id}")
            #     print(f"IP         : {self.host}")
            #     print(f"PORT       : {self.port}")
            #     print(f"INTRODUCER : {self.introducer}")
            elif arg == 'neighbours':
                print(self.neighbours)
            # else:
            #     log = f"[ERROR]: Receiver invalid argument in the Command Block : {arg}"
            #     print(log)
            #     # self.logsaver(logs=log)
    
    def print_membership_list(self):
        print("[INFO]: Membership List: ")
        for key in self.membershiplist.keys():
            print(f"{key}: [ status: {self.membershiplist[key]['status']}]")