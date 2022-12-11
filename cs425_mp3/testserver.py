from testfailuredetector import *
import os
import tqdm

class Server:
    def __init__(self):
        self.fd =  FailureDetector()

    def run(self):
        fd_thread = threading.Thread(target=self.fd.run)
        test_thread = threading.Thread(target=self.test)

        fd_thread.start()
        test_thread.start()

        fd_thread.join()
        test_thread.join()

    def send_file(self, filename , IP, PORT):
        filesize = os.path.getsize(filename)
        s = socket.socket()
        s.connect((IP, PORT))
        s.send(f"{filename} {filesize}".encode())

        progress = tqdm.tqdm(range(filesize), f"Sending {filename}", unit="B", unit_scale=True, unit_divisor=1024)
        with open(filename, "rb") as f:
            while True:
                bytes_read = f.read(4096)
                if not bytes_read:
                    break
                s.sendall(bytes_read)
                progress.update(len(bytes_read))
        
        s.close()
    
    def recv_file(self):
        s = socket.socket()
        s.bind((self.fd.host, 5001))
        s.listen(5)
        client, address = s.accept()
        received = client.recv(4096).decode()
        filename, filesize = received.split()
        filename = os.path.basename()
        filesize = int(filesize)
        progress = tqdm.tqdm(range(filesize), f"Receiving {filename}", unit="B", unit_scale=True, unit_divisor=1024)
        with open(filename, "wb") as f:
            while True:
                bytes_read = client.recv(4096)
                if not bytes_read:
                    break
                f.write(bytes_read)
                progress.update(len(bytes_read))
        client.close()
        s.close()
        


    def test(self):
        while True:
            time.sleep(5)
            # self.fd.print_membership_list()
            print(self.fd.neighbours)
            # print(self.fd.distance)



if __name__ == '__main__':
    s = Server()
    s.run()
    