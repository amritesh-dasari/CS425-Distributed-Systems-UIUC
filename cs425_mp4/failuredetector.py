from datetime import datetime
import json
import random
import socket
import threading
import time
from subprocess import check_output
from collections import OrderedDict

INTRODUCER = "fa22-cs425-7301.cs.illinois.edu"

class MembershipList:
    def __init__(self, host, id):
        self.membershiplist = OrderedDict()
        self.membershiplist = {
            host:{
                'id':id, 
                'timestamp':datetime.now().strftime('%H:%M:%S'), 
                'status': "INACTIVE"
            }
        }
class FailureDetector:
    def __init__(self):
        self.host = socket.gethostname()
        self.id = str(random.randint(0, 4096)) + "_" + self.host + "_"+ datetime.now().strftime('%H:%M:%S')
        self.port = 8080
        self.introducer = True if (self.host==INTRODUCER) else False
        self.mbslist = MembershipList(self.host, self.id)
        self.neighbours = []
        self.distance = []
        self.timers = {}
        # self.fs = {}
        self.membership_lock = threading.Lock()
        self.timer_lock = threading.Lock()
    
    def run(self):
        # Driver code to start listener and sender
        listener_thread = threading.Thread(target=self.listener)
        pinger_thread = threading.Thread(target=self.pinger)
        timer_thread = threading.Thread(target=self.timercheck)
        join_thread = threading.Thread(target=self.join)
        # command_thread = threading.Thread(target=self.command_func)

        listener_thread.start()
        pinger_thread.start()
        timer_thread.start()
        join_thread.start()
        # command_thread.start()
        
        listener_thread.join()
        pinger_thread.join()
        timer_thread.join()
        join_thread.join()
        # command_thread.join()
        
    def getmembership(self):
        return self.mbslist.membershiplist
    def list_self(self):
        self.logsaver(logs="[INFO]: Request received to print self")
        print(self.id)

    def update_neighbours(self):
        membershiplist = self.mbslist.membershiplist
        neighbours = self.neighbours
        distance = self.distance
        hostnum = int(self.host.split("-")[2][2:4])

        if len(neighbours)>0:
            temp=[]
            for index in range(len(neighbours)):
                if membershiplist[neighbours[index]]['status']!="ACTIVE":
                    temp.append(index)
            for index in temp:
                del neighbours[index]
                del distance[index]
        
        if len(neighbours)<4:
            for node in membershiplist.keys():
                if (node != self.host and membershiplist[node]['status'] == "ACTIVE" and node not in neighbours):
                    neighbours.append(node)
                    nodenum = int(node.split("-")[2][2:4])
                    distance.append(min(abs(hostnum-nodenum),10-abs(hostnum-nodenum)))
                    if len(neighbours)==4:
                        break
        
        for node in membershiplist.keys():
            if (node!=self.host and membershiplist[node]['status']=="ACTIVE" and node not in neighbours):
                tempnodenum = int(node.split("-")[2][2:4])
                tempdistance = min(abs(hostnum-tempnodenum), 10-abs((hostnum-tempnodenum)))
                currentmaxdistance = max(distance)
                if tempdistance>currentmaxdistance:
                    continue
                else:
                    for index in range(len(distance)):
                        if tempdistance<distance[index]:
                            del neighbours[index]
                            neighbours.append(node)
                            del distance[index]
                            distance.append(tempdistance)
                            break
        
        self.neighbours = neighbours
        self.distance = distance
        
    def print_timer_list(self):
        # Print Timers Dictionary
        self.logsaver(logs="[INFO]: Request received to print Timer list")  
        if len(self.timers.keys())>0:
            print("[INFO]: Timer List: \n")
            for (host,stopwatch) in list(self.timers.items()):
                print(f"[{host}]: {stopwatch}")
        else:
            print("[INFO]: Empty Timers list")

    def print_membership_list(self):
        # Print the membership list 
        membershiplist=self.mbslist.membershiplist
        self.logsaver(logs="[INFO]: Request received to print Membership List")
        print("[INFO]: Membership List: \n")
        for key in membershiplist.keys():
            print(f"{key}: [status: {membershiplist[key]['status']}]")

    def timercheck(self):
        membershiplist = self.mbslist.membershiplist
        while True: 
            try:
                self.timer_lock.acquire()
                temp=[]
                for host in self.timers.keys():
                    host_time  = self.timers[host]
                    checker_time = datetime.now()
                    time_difference = checker_time - host_time
                    failure_time = time_difference.seconds
                    if failure_time > 6:
                        if host not in membershiplist:
                            continue
                        if membershiplist[host]['status'] == "INACTIVE" or membershiplist[host]['status'] == "ERRORED":
                            continue
                        log = f"[ERROR]: Timeout on {host}"
                        #print(log)
                        self.logsaver(logs=log)

                        membershiplist[host]['status'] = "ERRORED"
                        membershiplist[host]['timestamp'] = checker_time.strftime('%H:%M:%S')
                        temp.append(host)
                for host in temp:
                    del self.timers[host]
                self.timer_lock.release()
                self.membership_lock.acquire()
                self.update_neighbours()
                self.membership_lock.release()
    
            except Exception as e:
                log = f"[ERROR]: Exception at timercheck {str(e)}, continuing Timercheck module"
                # print(log)
                self.logsaver(logs=log)
                continue

    def listener(self):
        # Listen for pings, process and send ack    
        membershiplist=self.mbslist.membershiplist
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection_listener:
            connection_listener.bind((self.host, self.port))
            while True:
                try:
                    packet, peer = connection_listener.recvfrom(4096)
                    if membershiplist[self.host]['status'] == "INACTIVE" or membershiplist[self.host]['status'] == "ERRORED":
                        continue
                    if packet:
                        data = json.loads(packet.decode('utf-8'))
                        packet_type = data.get('message_type','NA')
                        
                        if packet_type == "NA":
                            log = f"[ERROR]: Ignoring UDP packet from {data['host']}:{data['port']} containing no message type"
                            # print(log)
                            self.logsaver(logs=log, packet=data)
                            continue
                        
                        sender_host = data.get('host')
                        listener_timestamp = datetime.now().strftime('%H:%M:%S')
                        self.membership_lock.acquire()

                        # PING Message
                        if packet_type == "PING":
                            log = f"[INFO]: Recieved PING packet from {sender_host}"
                            #print(log)
                            # self.logsaver(logs=log, packet=data)
                            message = data['membershiplist']
                            for hostid in message:
                                if hostid not in membershiplist:
                                    membershiplist[hostid] = message[hostid]
                                    continue
                                sender_timestamp = datetime.strptime(message[hostid]['timestamp'], '%H:%M:%S')
                                local_timestamp = datetime.strptime(membershiplist[hostid]['timestamp'], '%H:%M:%S')
                                if(sender_timestamp > local_timestamp):
                                    membershiplist[hostid] = message[hostid]
                            # self.update_neighbours()
                            response_message = {
                                'message_type': 'ACK',
                                'host': self.host,
                                'port': self.port,
                                'membershiplist': membershiplist[self.host]
                            }
                            log = f"[INFO]: Sending ACK packet to {sender_host}"
                            # self.logsaver(logs=log, packet=response_message)
                            connection_listener.sendto(json.dumps(response_message).encode('utf-8'), (sender_host, 8080))

                        # ACK Message
                        elif packet_type == "ACK":
                            log = f"[INFO]: Received ACK packet from {sender_host}"
                            # self.logsaver(logs = log, packet = data)
                            if sender_host in self.timers:
                                self.timer_lock.acquire()
                                del self.timers[sender_host]
                                self.timer_lock.release()
                            membershiplist[sender_host] = data.get('membershiplist')


                        # JOIN Message
                        elif packet_type == "JOIN":
                            # add incoming member to local membership list
                            log = f"[INFO]: Received JOIN packet from {sender_host}"
                            # print(log)
                            self.logsaver(logs = log, packet = data)
                            membershiplist[sender_host] = data.get('membershiplist')
                            membershiplist[sender_host]['status'] = "ACTIVE" 
                            membershiplist[sender_host]['timestamp'] = listener_timestamp
                            log = f"[INFO]: Successfully updated Membership List"
                            self.logsaver(logs = log)
                            
                            # if introducer, broadcast JOIN message to rest of nodes
                            if self.introducer is True:
                                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as temp_connection:
                                    temp_connection.bind((self.host,8081))
                                    packet = {
                                        'membershiplist': membershiplist[sender_host],
                                        'host':sender_host,
                                        'port':'8080', 
                                        'message_type': "JOIN"
                                    }
                                    # send join messages to all nodes in current membership list
                                    for host in membershiplist.keys():
                                        if host != self.host:
                                            log = f"[INFO]: Sending JOIN packet from {sender_host} to {host}"
                                            # print(log)
                                            self.logsaver(logs = log, packet = packet)
                                            temp_connection.sendto(json.dumps(packet).encode('utf-8'), (host, 8080))

                                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as temp_connection:
                                    temp_connection.bind((self.host,8081))
                                    log = f"[INFO]: Sending JOIN_SDFS packet to {INTRODUCER} for {sender_host}"
                                    #print(log)
                                    packet = {
                                        'vm':sender_host,
                                        'host':self.host,
                                        'port':'8080', 
                                        'message_type': "JOIN_SDFS"
                                    }
                                    temp_connection.sendto(json.dumps(packet).encode('utf-8'), (INTRODUCER, 8100))

                            self.update_neighbours()
                        
                        # LEAVE Message
                        elif packet_type == "LEAVE":
                            log = f"[INFO]: Received LEAVE packet from {sender_host}"
                            print(log)
                            self.logsaver(logs=log, packet=data)
                            membershiplist[sender_host]['status'] = "INACTIVE"
                            membershiplist[sender_host]['timestamp'] = listener_timestamp
                            self.timer_lock.acquire()
                            if sender_host in self.timers.keys():
                                del self.timers[sender_host]
                            self.timer_lock.release()
                            if self.introducer is True:
                                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as temp_connection:
                                    temp_connection.bind((self.host,8081))
                                    packet = {
                                        'host':sender_host,
                                        'port':'8080', 
                                        'message_type': "LEAVE"
                                    }
                                    # send join messages to all nodes in current membership list
                                    for host in membershiplist.keys():
                                        if host != self.host and host != sender_host:
                                            log = f"[INFO]: Sending LEAVE packet from {sender_host} to {host}"
                                            # print(log)
                                            self.logsaver(logs = log, packet = packet)
                                            temp_connection.sendto(json.dumps(packet).encode('utf-8'), (host, 8080))
                            self.update_neighbours()

                        # Unknown Message
                        else:
                            log = f"[ERROR]: UDP packet received has unknown message type."
                            # print(log)
                            self.logsaver(logs=log, packet=data)
                        
                        self.membership_lock.release()
                        
                except Exception as e:
                    log = f"[ERROR]: Exception at Listener {str(e)}"
                    # print(log)
                    self.logsaver(logs=log)
                    continue
    
    def pinger(self):
        # Send PING and wait for ACK
        membershiplist = self.mbslist.membershiplist
        # Construct PING packet and send to neighbors
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
            connection.bind((self.host,8082))
            while True:
                try:
                    time.sleep(1.5)
                    if membershiplist[self.host]['status'] != "ACTIVE":
                        continue
                    now = datetime.now().strftime('%H:%M:%S')
                    self.membership_lock.acquire()
                    membershiplist[self.host]['status'] = "ACTIVE"
                    membershiplist[self.host]['timestamp'] = now
                    packet = {
                        'membershiplist': membershiplist,
                        'host':self.host,
                        'port':self.port, 
                        'message_type': "PING"
                    }

                    for receiver in self.neighbours:
                        if receiver not in membershiplist:
                            continue
                        if receiver in membershiplist and membershiplist[receiver]["status"] == "INACTIVE":
                            continue
                        if receiver in membershiplist and membershiplist[receiver]["status"] == "ERRORED":
                            continue
                        log = f"[INFO]: Sending PING packet to {receiver}"
                        # self.logsaver(logs=log, packet=packet)
                        connection.sendto(json.dumps(packet).encode('utf-8'), (receiver, 8080))

                        if receiver in membershiplist and receiver not in self.timers:
                            self.timer_lock.acquire()
                            self.timers[receiver] = datetime.now()
                            self.timer_lock.release()
                            log = f"[INFO]: Updating timer table for {receiver} with {self.timers[receiver]}"
                            # self.logsaver(logs=log)

                    self.membership_lock.release()
                except Exception as e:
                    log = f"[ERROR]: Exception at Sender {str(e)}"
                    # print(log)
                    self.logsaver(logs=log)
                    continue

    def join(self):
        # For nodes to join the membership list
        membershiplist = self.mbslist.membershiplist
        membershiplist[self.host]['timestamp'] = datetime.now().strftime('%H:%M:%S')
        membershiplist[self.host]['status'] = "JOINING"

        if self.introducer is False:
            # initialize connection
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
                connection.bind((self.host,8083))
                packet = {
                    'membershiplist': membershiplist[self.host],
                    'host':self.host,
                    'port':self.port, 
                    'message_type': "JOIN"
                }
                log = f"[INFO]: Sending JOIN packet to INTRODUCER: {INTRODUCER}"
                # print(log)
                self.logsaver(logs=log, packet=packet)
                connection.sendto(json.dumps(packet).encode('utf-8'), (INTRODUCER, 8080))
        else:
            membershiplist[self.host]['status'] = "ACTIVE"
        
        

    def leave(self):
        # For nodes to leave gracefully, without exceptions
        membershiplist = self.mbslist.membershiplist
        if membershiplist[self.host]['status'] != 'ACTIVE':
            log = f"[ERROR]: Cannot change status from {membershiplist[self.host]['status']} to 'INACTIVE' "
            # print(log)
            self.logsaver(logs=log)
            return

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
            connection.bind((self.host,8084))
            packet = {
                'port': self.port,
                'host': self.host,
                'message_type': 'LEAVE'
            }
            connection.sendto(json.dumps(packet).encode('utf-8'), (INTRODUCER, 8080))
            membershiplist[self.host]['status'] = "INACTIVE"
        log = f"[INFO]: Successfully left the network"
        # print(log)
        self.logsaver(logs=log)

    def logsaver(self, logs, packet = ""):
        filename = self.host.split("-")[2][2:4]
        file_handler = open(f"machine.{filename}.log","a")
        if packet != "":
            write_head = f"{datetime.now()} : {logs}\n Data_Packet : {packet}\n\n"
        else : 
            write_head = f"{datetime.now()} : {logs}\n\n"
        file_handler.write(write_head)
        file_handler.close()
        

    def grep(self, phrase="NA"):
        cmd_lst=[]
        if phrase!="NA" and phrase!="":
            cmd_lst.append("grep")
            cmd_lst.append(phrase)
        else:
            cmd_lst.append("cat")
        filename = self.host.split("-")[2][2:4]
        cmd_lst.append(f"machine.{filename}.log")
        print(f"[INFO] Command to be run: {cmd_lst}") 
        log_string = check_output(cmd_lst).decode("utf-8")
        print(f"Output: {log_string}")
        

    def command_func(self):
        # Command Block as in Minecraft Command Block
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
        elif arg == 'timer':
            self.print_timer_list()
        elif arg =='self':
            self.list_self()
        elif arg == 'leave':
            self.leave()
        elif arg == 'list':
            self.print_membership_list()
        elif arg == 'grep':
            phrase=input('Enter GREP phrase (Press Enter to do cat): ')
            self.grep(phrase)
        elif arg == 'details':
            self.logsaver(logs="[INFO]: Request for server details received", packet={"id":self.id, "host":self.host, "port":self.port, "introducer":self.introducer})
            print(f"ID         : {self.id}")
            print(f"IP         : {self.host}")
            print(f"PORT       : {self.port}")
            print(f"INTRODUCER : {self.introducer}")
        elif arg == 'neighbours':
            # self.update_neighbours()
            print(self.neighbours)
        else:
            log = f"[ERROR]: Receiver invalid argument in the Command Block : {arg}"
            print(log)
            self.logsaver(logs=log)
