from failuredetector import *
import os
import socket
import subprocess

SDFS_path = "/home/shikhar4/cs425-mp4/sdfs/"
class FileSystem:
	def __init__(self):
		# vmMapper: keeps a list of files that are on each VM ID
		# fileMapper: keeps track of files that have been inserted - 1) global version number, 2) global list of replicas file is located at
		self.vmMapper = {"fa22-cs425-7301.cs.illinois.edu":[], "fa22-cs425-7302.cs.illinois.edu":[], "fa22-cs425-7303.cs.illinois.edu":[], "fa22-cs425-7304.cs.illinois.edu":[], "fa22-cs425-7305.cs.illinois.edu":[], "fa22-cs425-7306.cs.illinois.edu":[], "fa22-cs425-7307.cs.illinois.edu":[], "fa22-cs425-7308.cs.illinois.edu":[], "fa22-cs425-7309.cs.illinois.edu":[], "fa22-cs425-7310.cs.illinois.edu":[]}
		self.fileMapper = {}

	def insertFile(self,sdfsfilename,id_list):
		for id in id_list:
			if sdfsfilename not in self.vmMapper[id]:
				self.vmMapper[id].append(sdfsfilename)
				log = f"Added {sdfsfilename} to {id} in VM Mapper"
				
		if sdfsfilename not in self.fileMapper:
			self.fileMapper[sdfsfilename] = {"version":1, "replicaList":id_list}
		else:
			self.fileMapper[sdfsfilename]["version"] += 1
			self.fileMapper[sdfsfilename]["replicaList"] = id_list
		log = f"Updated {sdfsfilename} in File Mapper"
		
		print(log)
	
	def deleteFile(self,sdfsfilename):
		for id in self.vmMapper:
			if sdfsfilename in self.vmMapper[id]:
				self.vmMapper[id].remove(sdfsfilename)
			log = f"Removed {sdfsfilename} from VM: {id} in VM Mapper"
			print(log)
				
		if sdfsfilename in self.fileMapper:
			del self.fileMapper[sdfsfilename]
			log = f"Removed {sdfsfilename} from File Mapper"
			
			print(log)

	def printFileMapper(self,sdfsfilename):
		replica_list = self.fileMapper[sdfsfilename]["replicaList"]
		log = f"File: {sdfsfilename} is stored at VMs: {replica_list}"
		
		print(log)

	def printVMMapper(self):
		#print(self.vmMapper)
		host = socket.gethostname()
		listFiles = self.vmMapper[host]
		log = f"List of local SDFS files: {listFiles}"
		
		print(log)

class Server:
	def __init__(self):
		self.host = socket.gethostname()
		self.port = 8100
		self.ack_list = {}
		self.fd =  FailureDetector()
		self.sdfs = FileSystem()
		self.sdfslock = threading.Lock()
		self.primary = "fa22-cs425-7302.cs.illinois.edu"
		self.tempmbslist = {}
		self.secondary = False

	def run(self):
		fd_thread = threading.Thread(target=self.fd.run)
		#cmdblk_thread = threading.Thread(target=self.command_bloc)
		listener_thread = threading.Thread(target=self.listener)
		repicaChecker_thread = threading.Thread(target=self.replicaChecker)
		leaderchecker_thread = threading.Thread(target=self.leaderChecker)
		amiAlive_thread = threading.Thread(target=self.amiAlive)
		
		fd_thread.start()
		#cmdblk_thread.start()
		listener_thread.start()
		repicaChecker_thread.start()
		leaderchecker_thread.start()
		amiAlive_thread.start() #Just this node having existential dread, don't worry about it
		
		fd_thread.join()
		#cmdblk_thread.join()
		listener_thread.join()
		repicaChecker_thread.join()
		leaderchecker_thread.join()
		amiAlive_thread.join()
		
	def getMembershipList(self):
		return self.fd.mbslist.membershiplist

	def leaderElection(self):
		log = f"Leader Election Method invoked..."
		
		print(log)
		if self.primary ==self.host and len(self.fd.mbslist.membershiplist)>1:
			membershiplistkeys=list(self.fd.mbslist.membershiplist)
			for index in range(len(membershiplistkeys)-1,0,-1):
				node = membershiplistkeys[index]
				if self.fd.mbslist.membershiplist[node]['status'] == "ACTIVE" and node !=self.host:
					log = f"Secondary Node selected: {node}"
					
					print(log)
					packet = {
						"primary":self.host,
						"message_type":"LEADERELECTION",
						"host":self.host
					}
					with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
						log = f"Secondary Leader elected: {node}, sending leader election packet"
						
						print(log)
						connection.sendto(json.dumps(packet).encode('utf-8'), (node, 8100))
					break

	def leaderChecker(self):
		while True:
			if self.secondary == True and self.fd.mbslist.membershiplist[self.host]['status'] == "ACTIVE":
				if self.fd.mbslist.membershiplist[self.primary]['status']!="ACTIVE":
					self.primary = self.host
					self.secondary = False
					self.leaderElection()


	def replicaChecker(self):
		while True:
			if self.fd.mbslist.membershiplist[self.host]['status'] != "ACTIVE":
				continue
			vmMapper = self.sdfs.vmMapper
			fileMapper = self.sdfs.fileMapper
			membershiplist = self.fd.mbslist.membershiplist
			# Send PING and wait for ACK
			counter = 0
			if self.fd.introducer == True:
				for vm in vmMapper:
					if vm in membershiplist and membershiplist[vm]['status']!="ACTIVE" and vmMapper[vm]!=[]:
						#print(f"Local Membership List before sleep: {membershiplist}")
						time.sleep(5)
						#print(f"Local Membership List after sleep: {membershiplist}")
						if membershiplist[vm]['status']=="ACTIVE":
							print(f"False Alert on vm: {vm}")
							break
						#Get the files on the failed nodes
						log = f"Detected VM:{vm} has failed."
						print(log)
						
						fileList = vmMapper[vm]
						log = f"Files to be replicated: {fileList}"
						print(log)
						introducer_neighbors = self.fd.neighbours
						new_replica = ''
						for fileName in fileList:
							old_replicas=fileMapper[fileName]["replicaList"]
							for node in introducer_neighbors:
								if node not in old_replicas:
									old_replicas.remove(vm)
									old_replicas.append(node)
									new_replica = node
									log = f"New node for {fileName} is {node}"
									
									print(log)
									break
							vmMapper[vm].remove(fileName)
							fileMapper[fileName]["replicaList"]=old_replicas
							getandupdate_thread = threading.Thread(target= self.getandupdate, args=(fileName,5,new_replica))
							getandupdate_thread.start()
							vmMapper[new_replica].append(fileName)
							self.ack_list[fileName] = []
							with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
								PUT_message = {'message_type': "PUT", 'sdfsfilename': fileName, 'replicas':old_replicas, "host":self.host, "port":self.port}
								# multicast this put operation to all active members of membership list?
								for host in membershiplist.keys():
									if membershiplist[host]["status"] == "ACTIVE" and host!=self.host:
										log = f"Sending SDFS PUT to {host}"
										
										# print(log)
										connection.sendto(json.dumps(PUT_message).encode('utf-8'), (host, 8100))
							getandupdate_thread.join()

	def amiAlive(self):
		while True:
			if self.fd.mbslist.membershiplist[self.host]['status']!="ACTIVE":
				self.sdfs.fileMapper = {}
				self.sdfs.vmMapper = {"fa22-cs425-7301.cs.illinois.edu":[], "fa22-cs425-7302.cs.illinois.edu":[], "fa22-cs425-7303.cs.illinois.edu":[], "fa22-cs425-7304.cs.illinois.edu":[], "fa22-cs425-7305.cs.illinois.edu":[], "fa22-cs425-7306.cs.illinois.edu":[], "fa22-cs425-7307.cs.illinois.edu":[], "fa22-cs425-7308.cs.illinois.edu":[], "fa22-cs425-7309.cs.illinois.edu":[], "fa22-cs425-7310.cs.illinois.edu":[]}
			
					
	def listener(self):
		sdfs = self.sdfs
		with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection_listener:
			connection_listener.bind((self.host, self.port))
			while True:
				try:
					packet, peer = connection_listener.recvfrom(4096)
					if packet:
						data = json.loads(packet.decode('utf-8'))
						packet_type = data.get('message_type','NA')
						if packet_type == "NA":
							log = f"[ERROR]: Ignoring UDP packet from {data['host']}:{data['port']} containing no message type"
							
							# print(log)
							continue
						sender_host = data.get('host')
						self.sdfslock.acquire()

						if packet_type == "PUT":
							log = f"[INFO]: Recieved SDFS PUT message from {sender_host}"
							
							sdfsfilename = data.get('sdfsfilename')
							replica_list = data.get('replicas')
							self.sdfs.insertFile(sdfsfilename,replica_list)
							if self.host in replica_list:
								packet={
									'message_type': "ACK",
									'host':self.host,
									'filename':sdfsfilename
								}
								with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
									log = f"Sending SDFS ACK to {sender_host}"
									
									# print(log)
									connection.sendto(json.dumps(packet).encode('utf-8'), (sender_host, 8100))

						elif packet_type == "LEADERELECTION":
							log = f"Received LeaderElection Message from {sender_host}"
							
							# print(log)
							self.primary = data.get('primary')
							self.secondary = True

						elif packet_type == "DELETE":
							log = f"[INFO]: Recieved SDFS DELETE message from {sender_host}"
							
							# print(log)
							sdfsfilename = data.get('sdfsfilename')
							sdfs.deleteFile(sdfsfilename)

						elif packet_type == "JOIN_SDFS":
							log = f"[INFO]: Recieved JOIN_SDFS message from {sender_host}"
							joined_vm = data.get('vm')
							#print(log)
							packet = {'message_type': "UPDATE_SDFS", 'host':self.host, 'vmMapper': self.sdfs.vmMapper, 'fileMapper':self.sdfs.fileMapper}
							with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
								log = f"Sending UPDATE_SDFS to {joined_vm}. vmMapper sent: {self.sdfs.vmMapper}, fileMapper sent: {self.sdfs.fileMapper}"
								#print(log)
								connection.sendto(json.dumps(packet).encode('utf-8'), (joined_vm, 8100))

						elif packet_type == "UPDATE_SDFS":
							vmMapper = data.get('vmMapper')
							fileMapper = data.get('fileMapper')
							#print(f"Received VMMAPPER: {vmMapper}, FILEMAPPER: {fileMapper} from {sender_host}")
							self.sdfs.vmMapper = vmMapper
							self.sdfs.fileMapper = fileMapper

						elif packet_type == "ACK":
							sdfsfilename = data.get('filename')
							self.ack_list[sdfsfilename].append(sender_host)
							log = f"Received ACK from {sender_host} for filename {sdfsfilename}"
							
							print(log)
							if len(self.ack_list[sdfsfilename]) > 3:
								log = f"Successful SDFS PUT for {sdfsfilename} with atleast 3 replicas"
								
								print(log)
						self.sdfslock.release()
				except Exception as e:
					log = f"[ERROR]: Exception at Listener :{repr(e)}"
					
					# print(log)
	
	def Filetransfer(self, replica_list,localfilename,sdfsfilename_versioned):
		for id in replica_list:
			self.send_file(localfilename,sdfsfilename_versioned,id)
	
	# put: insert / update 
	def put(self,localfilename,sdfsfilename):
		failureDetector = self.fd
		membershiplist = failureDetector.getmembership()
		self.sdfslock.acquire()
		# check if sdfsfilename has been inserted before - if yes, then this is an update operation
		if sdfsfilename in self.sdfs.fileMapper:
			latest_version = self.sdfs.fileMapper[sdfsfilename]["version"]
			replica_list = self.sdfs.fileMapper[sdfsfilename]["replicaList"]
		else:
			latest_version = 0
			replica_list = list(self.fd.neighbours)
		#send sdfs file to list of replicas for that file 
		sdfsfilename_versioned = sdfsfilename + "," + str(latest_version+1)
		print(f"Replica List for {sdfsfilename} is {replica_list}")
		filetransferThread = threading.Thread(target=self.Filetransfer, args=[replica_list, localfilename, sdfsfilename_versioned])
		filetransferThread.start()
		#update local sdfs
		self.sdfs.insertFile(sdfsfilename, replica_list)
		self.sdfslock.release()
		self.ack_list[sdfsfilename] = []
		with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
			PUT_message = {'message_type': "PUT", 'sdfsfilename': sdfsfilename, 'replicas':replica_list, "host":self.host, "port":self.port}
			# multicast this put operation to all active members of membership list?
			for host in membershiplist.keys():
				if membershiplist[host]["status"] == "ACTIVE" and host!=self.host:
					log = f"Sending SDFS PUT to {host}"
					
					print(log)
					connection.sendto(json.dumps(PUT_message).encode('utf-8'), (host, 8100))
		filetransferThread.join()

	def get(self,sdfsfilename,localfilename,version =0):
		fileMapper = self.sdfs.fileMapper
		failureDetector = self.fd
		membershiplist = failureDetector.getmembership()
		self.sdfslock.acquire()
		#with our list of replicas, we want to get the file from the first active replica
		latest_version = fileMapper[sdfsfilename]["version"]
		replica_list = fileMapper[sdfsfilename]["replicaList"]
		if version == 0:
			sdfsfilename_versioned = sdfsfilename + "," + str(latest_version)
		else:
			sdfsfilename_versioned = sdfsfilename + "," + str(version)
		self.sdfslock.release()
		log = f"GET: Obtaining file: {sdfsfilename_versioned}"
		
		print(log)
		for vm in replica_list:
			#check if in membership list and active -> if yes then perform recv_file from that host
			if vm in membershiplist and membershiplist[vm]['status'] == "ACTIVE":
				self.recv_file(sdfsfilename_versioned,localfilename,vm)
				break
			else:
				continue

	def delete(self,sdfsfilename):
		sdfs = self.sdfs
		membershipList = self.fd.mbslist.membershiplist
		sdfs.deleteFile(sdfsfilename)
		#broadcast delete message to all other active vms so they can update their file system as well
		with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
			DELETE_message = {'message_type': "DELETE", 'sdfsfilename': sdfsfilename}
			for host in membershipList: 
				if membershipList[host]["status"] == "ACTIVE" and host!=self.host:
					log = f"Sending SDFS DELETE to {host} for file: {sdfsfilename}"
					
					# print(log)
					connection.sendto(json.dumps(DELETE_message).encode('utf-8'), (host, 8100))

	def send_file(self, localfilename, sdfsfilename, VM):
		user = "shikhar4"
		#IP = "fa22-cs425-730{}.cs.illinois.edu".format(VM_num)
		target_filepath = os.path.join(SDFS_path,sdfsfilename)
		target = user + "@" + VM + ":" + target_filepath
		p = subprocess.Popen(['scp', localfilename, target])
		p.wait()
		log = f"Sent {localfilename} to {target} as {sdfsfilename}"
		
		#print(log)

	def recv_file(self, sdfsfilename, localfilename, VM):
		user = "shikhar4"
		#IP = "fa22-cs425-730{}.cs.illinois.edu".format(VM_num)
		target_filepath = os.path.join(SDFS_path, sdfsfilename)
		target = user + "@" + VM + ":" + target_filepath
		p = subprocess.Popen(['scp', target, localfilename])
		p.wait()
		log = f"Received {sdfsfilename} from {target} as {localfilename}"
		
		#print(log)
	
	def ls(self):
		cmd_lst = ["ls"]
		log_string = check_output(cmd_lst).decode("utf-8")
		print(f"Output: {log_string}")

	def cat(self, filename):
		cmd_lst = ["cat",filename]
		log_string = check_output(cmd_lst).decode("utf-8")
		print(f"Output: {log_string}")
	
	def getandupdate(self,sdfsfilename,versions, sendtoVM):
		if sdfsfilename in self.sdfs.fileMapper.keys():
			latest_version = self.sdfs.fileMapper[sdfsfilename]["version"]
			for version in range(max(latest_version-int(versions), 1),latest_version+1):
				self.get(sdfsfilename,sdfsfilename,version)
				log = f"Getting version: {version} for file: {sdfsfilename}"
				
				print(log)
				sdfsfilename_versioned = sdfsfilename + "," + str(version)
				self.send_file(sdfsfilename,sdfsfilename_versioned,sendtoVM)

	def getversionsOnefile(self, sdfsfilename, versions, localfilename):		
		if sdfsfilename in self.sdfs.fileMapper.keys():
			latest_version = self.sdfs.fileMapper[sdfsfilename]["version"]
			for version in range(latest_version, max(latest_version-int(versions), 0), -1):
				self.get(sdfsfilename,"temp.txt",version)
				writefile = open(localfilename,"a")
				readfile = open("temp.txt","r")
				writefile.write(f"Version: {version} \n")
				for pointer in readfile.readlines():
					writefile.write(pointer)
				writefile.write("\n\n")
				writefile.close()
				readfile.close()
			log = f"Received Files, Stored in local dir as: {localfilename}"
			
			print(log)
		else:
			log = f"{sdfsfilename} not found in SDFS, please check 'store' command"
			
			print(log)
	
	def command_bloc(self):
		# Command Block for all the essential functions of the SDFS
			
		try:
			arg = input("Enter Command Argument: ")
			if arg == "put":
				localfilename = input("Enter Local File Name: ")
				sdfsfilename = input("Enter SDFS File Name: ")
				self.put(localfilename, sdfsfilename)
			elif arg == "get":
				sdfsfilename = input("Enter File Name to be fetched from the SDFS: ")
				localfilename = input("Enter Local Filename: ")
				if sdfsfilename not in self.sdfs.fileMapper:
					print(f"SDFS FILE: {sdfsfilename} does not exist in SDFS")
				else:
					self.get(sdfsfilename, localfilename)
			elif arg == "delete":
				sdfsfilename = input("Enter File Name to delete: ")
				self.delete(sdfsfilename)
			elif arg == "ls":
				sdfsfilename = input("Enter file to ls: ")
				self.sdfs.printFileMapper(sdfsfilename)
			elif arg == "store":
				self.sdfs.printVMMapper()
			elif arg == "fd":
				print("Initializing Failure Detector Command Block...")
				fdcmdblck_thread = threading.Thread(target=self.fd.command_func)
				fdcmdblck_thread.start()
				fdcmdblck_thread.join()
				print("Exiting the Failure Detector Command Block...")
			elif arg == "files":
				self.ls()
			elif arg == "cat":
				filename = input("Enter Filename:")
				self.cat(filename)
			elif arg == "get-versions":
				sdfsfilename = input("Enter SDFS Filename: ")
				versions = input("Enter Versions: ")
				localfilename = input("Enter Local Filename: ")
				self.getversionsOnefile(sdfsfilename,versions,localfilename)
			elif arg =="leaderelection":
				print(f"Calling leader election protocol...")
				self.leaderElection()
			elif arg == "status":
				print(self.primary)
				print(self.secondary)
		except Exception as e:
			log = f"Error: {repr(e)}"
			
			print(log)

if __name__ == '__main__':
	s = Server()
	s.run()
