from server import *
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 
import numpy as np
import matplotlib.pyplot as plt
import pickle
from lenet_model import LeNetModel
import tensorflow as tf
from tensorflow import keras
from sklearn.model_selection import train_test_split
from keras.datasets import mnist
from keras.optimizers import SGD
from keras.utils import np_utils
from keras import backend as K
import pickle
import numpy as np
import argparse
import cv2
import sys
import h5py
import math
from resnets_utils import *

COORDINATOR = "fa22-cs425-7301.cs.illinois.edu"

class Query:
	def __init__(self):
		#querylist stores filename of dataset ML model will learn inference on
		self.file_list = []
		self.id = str(random.randint(0, 4096))
		self.type = None 

	def addFile(self,file_name):
		self.file_list.append(file_name)

	def setFileList(self, file_list):
		self.file_list = file_list
		#print(f"Set File List to {self.file_list} for Query {self.id}")

	def getList(self):
		return self.file_list

	def getID(self):
		return self.id

class IDunno:
	def __init__(self):
		#list of inputted queries to system. states: "IDLE" - query state when it has just been added to list when receiving query by client(listener thread), 
		#"ACTIVE" - query is being run on vms, no files for this job are waiting to be scheduled , "WAITING" - variation of ACTIVE state. when scheduling this query, all the files or some files did not find a VM to run on 
		#"FINISHED" - set in jobChecker when all files for this query have been processed, we ignore these when scheduling jobs
		self.query_list = {}
		self.failed_query_list = []
		self.batch_size = 1
		self.jobQueue = []
		self.query_to_model = {}
		self.model = None
		self.resnetdataset = None
		self.resnetxdata = None
		self.resnetydata = None
		self.host = socket.gethostname()
		self.port = 8200
		self.fd =  FailureDetector()
		self.sdfs_server = Server()
		self.running_count = 0
		self.last_running_count = 0
		self.tensecondcount = 0
		self.idunnolock = threading.Lock()
		self.running_job_flag = True
		self.c1count = 0
		self.current_coordinator = "fa22-cs425-7301.cs.illinois.edu"
		self.secondary_coordinator= "fa22-cs425-7302.cs.illinois.edu"


		#maps vm to query id being run on that vm, ALL OF THESE ARE USED ONLY BY COORDINATOR
		self.vm_to_query = {"fa22-cs425-7303.cs.illinois.edu":[], "fa22-cs425-7304.cs.illinois.edu":[], "fa22-cs425-7305.cs.illinois.edu":[], "fa22-cs425-7306.cs.illinois.edu":[], "fa22-cs425-7307.cs.illinois.edu":[], "fa22-cs425-7308.cs.illinois.edu":[], "fa22-cs425-7309.cs.illinois.edu":[], "fa22-cs425-7310.cs.illinois.edu":[]}
		self.query_to_vm = {}
		self.query_to_files = {}
		self.vm_to_file = {"fa22-cs425-7303.cs.illinois.edu":[], "fa22-cs425-7304.cs.illinois.edu":[], "fa22-cs425-7305.cs.illinois.edu":[], "fa22-cs425-7306.cs.illinois.edu":[], "fa22-cs425-7307.cs.illinois.edu":[], "fa22-cs425-7308.cs.illinois.edu":[], "fa22-cs425-7309.cs.illinois.edu":[], "fa22-cs425-7310.cs.illinois.edu":[]}
		self.query_to_time = {}
		#[average,standard dev]
		self.model_statistics = {"lenet":{"average":0, "std":0}, "resnet":{"average":0, "std":0}}
		self.query_to_count = {}
		#key: query_id, value:[list of files that have been processed by worker]
		self.activeQueriesFinishedFiles = {}
		self.activeQueries = []
		self.numActiveJobs = 0

	def run(self):
		cmdblk_thread = threading.Thread(target=self.command_bloc)
		listener_thread = threading.Thread(target=self.listener)
		jobchecker_thread = threading.Thread(target=self.jobChecker)
		failedjob_thread = threading.Thread(target = self.failedJobDetector)
		sdfs_server_thread = threading.Thread(target=self.sdfs_server.run)
		runjobthread = threading.Thread(target=self.runJobListener)
		failedcoordinator = threading.Thread(target=self.failedCoordListener)
		

		cmdblk_thread.start()
		listener_thread.start()
		jobchecker_thread.start()
		failedjob_thread.start()
		runjobthread.start()
		failedcoordinator.start()
		sdfs_server_thread.start()

		
		listener_thread.join()
		jobchecker_thread.join()
		failedjob_thread.join()
		runjobthread.join()
		failedcoordinator.join()
		sdfs_server_thread.join()
		cmdblk_thread.join()

		
	def addQuery(self,query, state):
		#query_list[query/query_id] = "IDLE"
		self.query_list[query] = state

	#takes in Query class and returns string for which model to use
	def mapQueryToModel(self,query_id):
		fileList = self.query_to_files[query_id]
		sample = fileList[0]
		if (sample.split("_")[0] == "mnist"):
			self.query_to_model[query_id] = "lenet"
		else: 
			self.query_to_model[query_id] = "resnet"
 
	def loadModel(self,model_name,filename):
		print(f"Loading {model_name} model into IDunno System")
		if model_name == "lenet":
			opt = SGD(learning_rate=0.01)
			self.model = LeNetModel.build(numChannels=1, imgRows=28, imgCols=28,
				numClasses=10,
				weightsPath="lenet_weights.hdf5")
			self.model.compile(loss="categorical_crossentropy", optimizer=opt,
				metrics=["accuracy"])
		elif model_name == "resnet":
			
			self.model = keras.models.load_model("resnet_model.h5")
			
			self.resnetdataset = h5py.File(filename, "r")
			
			self.resnetxdata = np.array(self.resnetdataset["test_set_x"][:])/255.
			self.resnetydata = convert_to_one_hot(np.array(self.resnetdataset["test_set_y"][:]), 6).T
			
	def failedCoordListener(self):
		while True:
			membershiplist = self.sdfs_server.getMembershipList()
			if membershiplist[self.host]['status'] != "ACTIVE":
				continue
			if self.host == self.secondary_coordinator:
				#print(membershiplist)
				if len(membershiplist) !=0:
					if self.current_coordinator not in membershiplist:
						continue
					if membershiplist[self.current_coordinator]['status'] != "ACTIVE":
						time.sleep(3)
						if membershiplist[self.current_coordinator]['status']=="ACTIVE":
							print(f"False Alert on Coordinator: {self.current_coordinator}")
							break
						print(f"Detected Failed Coordiator on VM: {self.current_coordinator}")
						self.current_coordinator = self.host
						for vm in membershiplist.keys():
							if membershiplist[vm]['status']=="ACTIVE":
								with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
									failed_coord_msg = {'message_type':"FAILED_COORDINATOR", "host":self.host, "port":self.port}
									log = f"Sending FAILED_COORDINATOR packet to {vm} "
									print(log)
									#uncomment
									connection.sendto(json.dumps(failed_coord_msg).encode('utf-8'), (vm, 8300))

					

	def sendJob(self,filename, model, vm,query_id):
		self.query_to_time[query_id]["time"] = datetime.now()
		with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
			JOB_message = {'message_type':"SEND_JOB", 'filename':filename, 'model':model, "host":self.host, "port":self.port, "query_id":query_id}
			secondary_message = {'message_type':"START_TIME", "host":self.host, "port":self.port, "query_id":query_id}
			log = f"Sending JOB to {vm} for filename: {filename} for query: {query_id}"
			print(log)
			#uncomment 
			connection.sendto(json.dumps(JOB_message).encode('utf-8'), (vm, 8200))
			if self.current_coordinator != self.secondary_coordinator:
				connection.sendto(json.dumps(secondary_message).encode('utf-8'), (self.secondary_coordinator, 8200))

	def finishJob(self,filename, query_id):
		self.running_count = 0
		self.tensecondcount=0
		self.last_running_count = 0
		with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
			FINISH_JOB_message = {'message_type':"FINISH_JOB", "host":self.host, "port":self.port, "filename": filename, "query_id":query_id}
			log = f"Sending FINISH_JOB to {self.current_coordinator} and {self.secondary_coordinator} for query: {query_id}"
			print(log)
			#uncomment
			connection.sendto(json.dumps(FINISH_JOB_message).encode('utf-8'), (self.current_coordinator, 8200))
			if self.current_coordinator != self.secondary_coordinator:
				connection.sendto(json.dumps(FINISH_JOB_message).encode('utf-8'), (self.secondary_coordinator, 8200))

	def removeQueryFromVM(self,query_id,vm,filename):
		self.idunnolock.acquire()
		self.vm_to_query[vm].remove(query_id)
		
		self.vm_to_file[vm].remove(filename)
		self.query_to_vm[query_id].remove(vm)
		self.idunnolock.release()
		#print(f"Updated VM to Query: {self.vm_to_query}")
		#print(f"Updated Query to VM: {self.query_to_vm}")

	def addQueryToVM(self,query_id,vm,filename):
		self.idunnolock.acquire()
		self.vm_to_query[vm].append(query_id)
		
		self.idunnolock.release()
		self.query_to_vm[query_id].append(vm)
		self.vm_to_file[vm].append(filename)
		#print(f"Updated VM to Query: {self.vm_to_query}")
		#print(f"Updated Query to VM: {self.query_to_vm}")

	#returns a vm to use
	def loadbalancer(self,membershiplist):
		#first pass through to see if vms have zero queries on 
		for vm in self.vm_to_query:
			if vm not in membershiplist:
				continue
			if len(self.vm_to_query[vm]) == 0 and membershiplist[vm]["status"] == "ACTIVE":
				return vm
		print("Couldn't find a free VM for scheduling")
		return 0

	def failedJobDetector(self):
		while True:
			membershiplist = self.sdfs_server.getMembershipList()
			if membershiplist[self.host]['status'] != "ACTIVE":
				continue
			if self.host == self.current_coordinator or self.host == self.secondary_coordinator:
				for failed_vm in self.vm_to_query:
					if failed_vm in membershiplist and membershiplist[failed_vm]["status"] != "ACTIVE" and self.vm_to_query[failed_vm] != []:
						print(f"Detected Failed Job on VM: {failed_vm}")
						time.sleep(5)
						if membershiplist[failed_vm]['status']=="ACTIVE":
							print(f"False Alert on vm: {failed_vm}")
							break
						for filename in self.vm_to_file[failed_vm]:
							for query_id in self.vm_to_query[failed_vm]:
								queryFileList = self.query_to_files[query_id]
								#query_id = query.getID()
								# self.query_to_time[query_id]["status"] = "FAILED"
								# now = datetime.now()
								# diff = self.query_to_time[query_id]["time"] - now
								# self.query_to_time[query_id]["time"] = diff
								if filename in queryFileList:
									model = self.query_to_model[query_id]
									self.removeQueryFromVM(query_id,failed_vm,filename)
									vm = self.loadbalancer(membershiplist)
									if vm == 0:
										print(f"Couldn't find replacement for File: {filename} Query: {query_id} on vm: {failed_vm}. Setting {query_id} to WAITING")
										self.query_list[query_id] = "WAITING"
										return
									print(f"Found Replacement for File: {filename} for Query: {query_id} on vm: {vm}")
									self.addQueryToVM(query_id,vm,filename)
									if self.host == self.current_coordinator:
										self.sendJob(filename, model,vm,query_id)
									break
	
	def scheduleJobsv2(self):
		#list of inputted queries to system. states: "IDLE" - query state when it has just been added to list when receiving query by client(listener thread), 
		#"ACTIVE" - query is being run on vms, no files for this job are waiting to be scheduled , "WAITING" - variation of ACTIVE state. when scheduling this query, all the files or some files did not find a VM to run on 
		#"FINISHED" - set in jobChecker when all files for this query have been processed, we ignore these when scheduling jobs
		membershiplist = self.sdfs_server.getMembershipList()
		if self.host == self.current_coordinator or self.host == self.secondary_coordinator:
			if len(self.query_list) != 0:

				for query_id in self.query_list:
					#query_id = query.getID()
					fileList = self.query_to_files[query_id]
					#print(f"Obtained file list: {fileList} for query: {query_id}")
					if self.query_list[query_id] == "FINISHED":
						if self.host == self.current_coordinator:
							print(f"{query_id} is FINISHED. Do not need to schedule")
						continue
					elif self.query_list[query_id] == "ACTIVE":
						if self.host == self.current_coordinator:
							print(f"{query_id} is ACTIVE. Do not need to schedule")
						continue
					elif self.query_list[query_id] == "IDLE":
						if self.host == self.current_coordinator:
							print(f"{query_id} in IDLE state. Attemping to schedule")
						for filename in fileList:
							vm = self.loadbalancer(membershiplist)
							if vm == 0:
								self.query_list[query_id] = "WAITING"
								print(f"Could not find a VM for file:{filename} for query: {query_id}. Set {query_id} to WAITING")
								#self.query_to_time[query_id]["time"] = datetime.now()
								return
							model = self.query_to_model[query_id]
							self.addQueryToVM(query_id,vm,filename)
							if self.host == self.current_coordinator:
								self.sendJob(filename, model,vm,query_id)
						self.activeQueries.append(query_id)
						self.numActiveJobs += 1
						self.query_list[query_id] = "ACTIVE"
						self.query_to_time[query_id]["status"] = "ACTIVE" 
						#print(f"Currently Active Queries: {self.activeQueries}")
						#print(f"Current Query List: {self.query_list}")

					elif self.query_list[query_id] == "WAITING":
						if self.host == self.current_coordinator:
							print(f"{query_id} in WAITING state. Attemping to schedule")
						for filename in fileList:
							activeFile = False
							for vm in self.vm_to_file:
								if filename in self.vm_to_file[vm] and query_id in self.vm_to_query[vm]:
									print(f"{filename} is already active on {vm} for same query: {query_id}, skipping this file")
									activeFile = True
							if activeFile:
								continue
							vm = self.loadbalancer(membershiplist)
							if vm == 0:
								print(f"Still waiting. Could not find a VM for file:{filename} for query: {query_id}")
								return
							model = self.query_to_model[query_id]
							self.addQueryToVM(query_id,vm,filename)
							if self.host == self.current_coordinator:
								self.sendJob(filename, model,vm,query_id)
						if query_id not in self.activeQueries:
							self.activeQueries.append(query_id)
						self.numActiveJobs += 1
						self.query_list[query_id] = "ACTIVE"
						#print(f"Currently Active Queries: {self.activeQueries}")
						#print(f"Current Query List: {self.query_list}")
					else:
						print(f"invalid query state")
			else:
				print(f"No queries available to schedule")

	def runModel(self, filename,query_id,model_name):
		print(f"Starting model: {model_name}")
		
		if model_name == "lenet":
			with open(filename, "br") as fh:
				data = pickle.load(fh)

			testData = data[0] 
			testLabels = data[1]
			model = self.model
			localfilename = f"{query_id}_{filename}"
			writefile = open(localfilename,"a")
			start = datetime.now()
			for i in range(0,400):
				self.running_count += 1
				self.tensecondcount += 1
				end = datetime.now()
				diff = end - start
				if diff.seconds > 22:
					self.tensecondcount = 0
					start = datetime.now()
				probs = model.predict(testData[np.newaxis, i], verbose = 0)
				prediction = probs.argmax(axis=1)
				log = ("[INFO] Predicted: {}, Actual: {}".format(prediction[0],
					np.argmax(testLabels[i])))
				writefile.write(f"{log}")
				writefile.write("\n\n")
				
			print(f"Results written to {localfilename}")
			writefile.close()

			return localfilename
		else:
			print("doing resnet")
			testData = self.resnetxdata
			testLabels = self.resnetydata
			model = self.model
			localfilename = f"{query_id}_{filename}"
			writefile = open(localfilename,"a")
			for j in range(0,3):
				for i in range(0,50):
					self.running_count += 1
					probs = model.predict(testData[np.newaxis, i], verbose = 0)
					prediction = probs.argmax(axis=1)
					log = ("[INFO] Predicted: {}, Actual: {}".format(prediction[0],
						np.argmax(testLabels[i])))
					writefile.write(f"{log}")
					writefile.write("\n\n")
					
			print(f"Results written to {localfilename}")
			writefile.close()

			return localfilename

	def runJob(self, filename, model, query_id):
		self.loadModel(model,filename)
		local_filename = self.runModel(filename,query_id,model)
		#self.running_job_flag = False
		sdfs_filename = local_filename + "_" +"sdfs.txt"
		self.sdfs_server.put(local_filename,sdfs_filename)

	def runJobListener(self): 
		with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection_listener:
			connection_listener.bind((self.host, 8300))
			while True:
				packet, peer = connection_listener.recvfrom(4096)
				if packet:
					data = json.loads(packet.decode('utf-8'))
					packet_type = data.get('message_type','NA')
					sender_host = data.get('host')
					if packet_type == "QUERY_COUNT":
						log = f"[INFO]: Received QUERY_COUNT from {sender_host}"
						print(log) 
						query_id = data.get('query_id')
						model = data.get('model')
						#print(self.running_count)
						send_count = self.running_count - self.last_running_count
						response_message = {'message_type': 'ACK_QUERY_COUNT','host': self.host,'port': self.port, 'tensecond':self.tensecondcount, 'count':send_count,'total_count':self.running_count,'query_id':query_id, 'model':model}
						connection_listener.sendto(json.dumps(response_message).encode('utf-8'), (sender_host, 8200))
						if self.current_coordinator != self.secondary_coordinator:
							connection_listener.sendto(json.dumps(response_message).encode('utf-8'), (self.secondary_coordinator, 8200))
						self.last_running_count = self.running_count
					
					elif packet_type == "FAILED_COORDINATOR":
						self.current_coordinator = self.secondary_coordinator
						print(f"Set Current Coordinator to {self.current_coordinator}")
		print("Exiting runJobThread listener loop")

	def runJobThread(self, filename, model, query_id):
		#self.running_job_flag = True
		self.runJob(filename,model,query_id)

	def listener(self):
		with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection_listener:
			connection_listener.bind((self.host, self.port))
			while True:
				try:
					packet, peer = connection_listener.recvfrom(4096)
					if packet:
						data = json.loads(packet.decode('utf-8'))
						packet_type = data.get('message_type','NA')
						sender_host = data.get('host')

						if packet_type == "NA":
							log = f"[ERROR]: Ignoring UDP packet from {data['host']}:{data['port']} containing no message type"
							print(log)
							continue

						elif packet_type == "QUERY": 
							log = f"[INFO]: Received QUERY from {sender_host}"
							print(log)
							fileList = data.get('fileList')
							#print(f"Received file list {fileList}")
							query = Query()
							query.setFileList(fileList)
							query_id = query.getID()
							self.query_to_files[query_id] = fileList
							self.query_list[query_id] = "IDLE"
							self.mapQueryToModel(query_id)
							self.query_to_vm[query_id] = []
							self.query_to_time[query_id] = {"status":"IDLE", "time":0}
							response_message = {'message_type': 'ACK_query','host': self.host,'port': self.port, 'query_id':query_id, 'fileList':fileList}
							connection_listener.sendto(json.dumps(response_message).encode('utf-8'), (sender_host, 8200))
							print(f"Sent ACK to {sender_host}")

						elif packet_type == "ACK_query":
							log = f"[INFO]: Received ACK_query from {sender_host}"
							query_id = data.get('query_id')
							fileList = data.get('fileList')
							##secondary coordinator
							self.query_to_files[query_id] = fileList
							self.query_list[query_id] = "IDLE"
							self.mapQueryToModel(query_id)
							self.query_to_vm[query_id] = []
							self.query_to_time[query_id] = {"status":"IDLE", "time":0}
							self.scheduleJobsv2()
							##secondary coordinator
							print(log)
							print(f"Coordinator successfully added query to system")

						elif packet_type == "ACK_QUERY_COUNT":
							log = f"[INFO]: Received ACK_QUERY_COUNT from {sender_host}"
							print(log)
							count = data.get('count')
							query_id = data.get('query_id')
							model = data.get('model')
							total_count = data.get('total_count')
							tensecondcount = data.get('tensecond')
							if query_id not in self.query_to_count:
								self.query_to_count[query_id] = 0
							self.query_to_count[query_id] += count
							#print(self.query_to_count)
							if model == "resnet":
								count += 20
							rate = math.floor(count/10)
							print(f"(i) Current query rate for {query_id} is : {tensecondcount}")
							print(f"(ii) Number of queries processed for {query_id} is: {self.query_to_count[query_id]}")

						elif packet_type == "START_TIME":
							query_id = data.get('query_id')
							self.query_to_time[query_id]["time"] = datetime.now()

						elif packet_type == "REQUEST_JOB":
							log = f"[INFO]: Received JOB REQUEST from {sender_host}"
							print(log)
							self.scheduleJobsv2()

						elif packet_type == "SEND_JOB":
							log = f"[INFO]: Received JOB from {sender_host}"
							print(log)
							filename = data.get('filename')
							model = data.get('model')
							query_id = data.get('query_id')

							
							#self.runJobThread(filename, model, query_id)
	
							self.runJob(filename, model, query_id)
							self.finishJob(filename,query_id)

						elif packet_type == "FINISH_JOB":
							filename = data.get('filename')
							finished_query_id = data.get('query_id')
							log = f"[INFO]: Received FINISH_JOB from {sender_host} for file:{filename} for query: {finished_query_id}"
							print(log)

							self.idunnolock.acquire()
							for query_id in self.vm_to_query[sender_host]:
								if query_id == finished_query_id:
									self.vm_to_query[sender_host].remove(query_id)
									self.query_to_files[query_id].remove(filename)
									#print(f"Filelist for {query_id}: {fileList}")
									#fileList.remove(filename)
									#query.setFileList(fileList)
							self.vm_to_file[sender_host].remove(filename)
							self.idunnolock.release()
							self.query_to_vm[finished_query_id].remove(sender_host)

							
							print(f"Scheduling any pending jobs")
							self.scheduleJobsv2()
							
							

				except Exception as e:
					log = f"[ERROR]: Exception at Listener :{repr(e)}"
					print(log)

	def jobChecker(self):
		while True:
			if self.numActiveJobs != 0 and (self.host == self.current_coordinator or self.host == self.secondary_coordinator):
				activeQueriesCopy = list(self.activeQueries)
				
				for query_id in activeQueriesCopy:
					#query_id = query.getID()
					currentFileList = self.query_to_files[query_id]
 
					#this job has been completed as all worker nodes have responded. 
					if len(currentFileList) == 0:
						print(f"Query: {query_id} has finished. Removing from IDunno system")
						self.query_to_time[query_id]["status"] = "FINISHED"
						if self.host == self.current_coordinator:
							orig_time = self.query_to_time[query_id]["time"]
							diff = datetime.now() - orig_time
							self.query_to_time[query_id]["time"] = diff
						for activequeryid in activeQueriesCopy:
							if activequeryid == query_id:
								self.activeQueries.remove(activequeryid)
						model = self.query_to_model[query_id]
						average = self.calculateAverage(model)
						stddev= self.calculateDev(average,model)
						self.query_list[query_id] = "FINISHED"
						self.numActiveJobs = len(self.activeQueries)
						#print(f"Currently Active Queries:{self.activeQueries}")
						#print(f"Current number of active jobs:{self.numActiveJobs}")
						break

	def training_phase(self):
		while True:
			try:
				arg = input("Enter Training Phase Command(query,exit): ")
				if arg == "query":
					fileList = []
					for i in range(0,self.batch_size):
						file_name = input("Enter file name: ")
						fileList.append(file_name)
					with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
						QUERY_message = {'message_type':"QUERY", "host":self.host, "port":self.port, "fileList": fileList}
						log = f"Sending QUERY_message to {self.current_coordinator}"
						print(log)
						#uncomment
						connection.sendto(json.dumps(QUERY_message).encode('utf-8'), (self.current_coordinator, 8200))
					#todo - add code to broadcast files vms must retrieve from sdfd based on model name 
					
				if arg == "exit":
					break
			except Exception as e:
				log = f"Error in training phase: {repr(e)}"
				print(log)

	def inference_phase(self):
		while True:
			try:
				arg = input("Enter Inference Phase Command(run_job,exit): ")
				if arg == "run_job":
					with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
						JOB_REQUEST_message = {'message_type':"REQUEST_JOB", "host":self.host, "port":self.port}
						log = f"Sending job request to {self.current_coordinator}"
						print(log)
						
						connection.sendto(json.dumps(JOB_REQUEST_message).encode('utf-8'), (self.current_coordinator, 8200))
				if arg == "exit":
					break
			except Exception as e:
				log = f"Error in inference phase: {repr(e)}"
				print(log)	

	def calculateAverage(self,model):
		res = 0
		sumtime = 0
		count = 0
		for queryid in self.query_to_time:
			stats = self.query_to_time[queryid]
			if stats["status"] == "FINISHED" and self.query_to_model[queryid] == model:
				count += 1
				if stats["time"] != 0:
					sumtime += stats["time"].seconds
		if count != 0:
			res = sumtime / count
			self.model_statistics[model]["average"] = res
		return res
	
	def calculateDev(self,average,model):
		count = 0
		numerator = 0
		res = 0
		for queryid in self.query_to_time:
			stats = self.query_to_time[queryid]
			if stats["status"] == "FINISHED" and self.query_to_model[queryid] == model:
				count += 1
				query_time = stats["time"].seconds
				numerator += (query_time - average)**2

		if count != 0:
			res = math.sqrt(numerator / count)
			self.model_statistics[model]["std"] = res
		return res

	def command_bloc(self):
		# Command Block 
		while True:
			try:
				arg = input("Enter IDunno Command Argument (batchsize, training, inference): ")
				if arg == "batchsize":
					size = input("Enter Desired Batch Size: ")
					self.batch_size = int(size)
				
				elif arg == "loading":
					print(f"Loading files into SDFS...") 
					self.sdfs_server.put("lenet_weights.hdf5", "sdfs_lenet_weights.hdf5")
					self.sdfs.server.put("mnist_testdata.pkl", "sdfs_mnist_testdata.pkl")
					self.sdfs.server.put("mnist_testdata2.pkl", "sdfs_mnist_testdata2.pkl")
					self.sdfs.server.put("mnist_testdata3.pkl", "sdfs_mnist_testdata3.pkl")
					print(f"Exiting loading phase...")

				elif arg == "training":
					print(f"Intializing Training Phase")
					training_thread = threading.Thread(target=self.training_phase)
					training_thread.start()
					training_thread.join() 
					print(f"Stopping Training Phase")

				elif arg == "inference":
					print(f"Intializing Inference Phase")
					inference_thread = threading.Thread(target=self.inference_phase)
					inference_thread.start()
					inference_thread.join() 
					print(f"Stopping Inference Phase")
					pass

				elif arg == "sdfs":
					print("Initializing SDFS Command Block...")
					sdfscmdblock_thread = threading.Thread(target=self.sdfs_server.command_bloc)
					sdfscmdblock_thread.start()
					sdfscmdblock_thread.join()
					print("Exiting the SDFS Command Block...")

				elif arg == "c1": 
					id_num = input("Enter query id you want to see query rate for: ")
					if id_num not in self.query_to_count:
						self.query_to_count[id_num] = 0
					with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
						for vm in self.vm_to_query:
							for query_id in self.vm_to_query[vm]:
								if query_id == id_num:
									model = self.query_to_model[id_num] 
									print(f"Getting query rate for VM: {vm}")
									packet = {'message_type': "QUERY_COUNT", "host":self.host, "port":self.port, "query_id":id_num, "model": model}
									connection.sendto(json.dumps(packet).encode('utf-8'), (vm, 8300))

				elif arg == "c2":
					id_num = input("Enter query id you want to see processing time for: ")
					stats = self.query_to_time[id_num]
					status = stats["status"]
					time = stats["time"]
					model = self.query_to_model[id_num]
					average = self.model_statistics[model]["average"]
					stddev = self.model_statistics[model]["std"]
					#stddev= self.calculateDev(average,model)

					print(f"Average Processing Time for Model {model} is : {average} seconds")
					print(f"STD DEV Processing Time for Model {model} is : {stddev} seconds")
					if status == "ACTIVE":
						now = datetime.now()
						difference = now - time
						print(f"Query {id_num} is currently running. Current Processing Time: {difference.seconds} seconds")
					elif status == "FINISHED":
						print(f"Query {id_num} has finished. Processing Time: {time.seconds} seconds")
					elif status == "FAILED":
						print(f"Query {id_num} failed. Processing Time:{time.seconds}")
					
				elif arg == "c4":
					print("Enter file you would like to view: ")
					filename = input("Enter File Name: ")
					self.sdfs_server.cat(filename)
				elif arg == "c5": 
					print(self.vm_to_query)
				elif arg == "querytime":
					print(self.query_to_time)
				elif arg == "querylist":
					#print(self.query_list)
					for query_id in self.query_list:
						#id_num = query.getID()
						status = self.query_list[query_id]
						fileslist = self.query_to_files[query_id]
						print(f"Query {query_id} has status {status}")
						print(f"Query {query_id} has active files: {fileslist}")
				elif arg == "modelstats":
					print(self.model_statistics)
				elif arg == "querytomodel":
					print(self.query_to_model)
				elif arg == "loadedmodel":
					print(self.model)
				elif arg == "numactive":
					print(self.numActiveJobs)
				elif arg == "activequeries":
					print(self.activeQueries)
				elif arg == "finishedfiles":
					query_id = input("Enter query id for which you want to see finished files for: ")
					print(self.activeQueriesFinishedFiles[query_id])
				elif arg == "membership":
					memlist = self.sdfs_server.getMembershipList()
					print(memlist)
				elif arg == "runjobflag":
					print(self.running_job_flag)

			except Exception as e:
				log = f"Error: {repr(e)}"
				print(log)

if __name__ == '__main__':
	idunno = IDunno()
	idunno.run()