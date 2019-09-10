import socket 
import sys 
import datetime 
from dateutil import parser 
from time import sleep
import threading

proxy_ip = '127.0.0.1'
proxy_port = 10000

class Client(object):
    def __init__(self, delta, ro, drift_direction):
        self.delta = delta
        self.ro = ro
        self.delta_t = delta/(2*ro)
        self.pos_drift = drift_direction
        self.last_sync_sys_time = datetime.datetime.now()
        self.last_sync_sim_time = self.last_sync_sys_time

    def drifted_time(self):
        current_sys_time = datetime.datetime.now()
        diff = (current_sys_time  - self.last_sync_sys_time).total_seconds()
        if self.pos_drift == 'pos':
            return self.last_sync_sim_time + datetime.timedelta(seconds=diff*(1+self.ro))
        else:
            return self.last_sync_sim_time + datetime.timedelta(seconds=diff*(1-self.ro))
        
    def reset_sim_vars(self, sim_time):
        self.last_sync_sim_time = sim_time
        self.last_sync_sys_time = datetime.datetime.now()
        
    # function used to Synchronize client process time 
    def synchronizeTime(self):
        s = socket.socket()      
        
        T0 = self.drifted_time()
        s.connect((proxy_ip, proxy_port))
        server_time = parser.parse(s.recv(1024).decode()) 
        T1 = self.drifted_time()  
        
        process_delay_latency = (T1 - T0).total_seconds()
        sync_client_time = server_time + datetime.timedelta(seconds = (process_delay_latency) / 2) 
        
        self.reset_sim_vars(sync_client_time)
        
        print("Time returned by server: " + str(server_time)) 
        print("Actual clock time at client side: " + str(T1)) 
        print("Synchronized process client time: " + str(sync_client_time)) 
        
        # calculate synchronization error  
        error = T1 - sync_client_time 
        print("Synchronization error : " + str(error.total_seconds()) + " seconds") 
        print('\n\n\n')
      
        s.close()
    

# python client.py delta ro drift_direction outputfile start_log_time
def main(): 
    client = Client(float(sys.argv[1]), float(sys.argv[2]), str(sys.argv[3]))
    
    # synchronize time using clock server forever
    while True:  
        sync_request = threading.Thread(target=client.synchronizeTime, args=())
        sync_request.start()
        sleep(client.delta_t)

        
if __name__ == "__main__":
    main()  
    

  

    
  
