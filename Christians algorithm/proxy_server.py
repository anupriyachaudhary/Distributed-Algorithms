import socket
import sys 
from random import uniform
from time import sleep
import threading

server_ip = '127.0.0.1'
server_port = 8000

def connectToServer(connection, max_Delay):
    # create delay
    delay = uniform(0, max_Delay/2)
    sleep(delay)
    print('delay = ', delay)
   
    # create new connection to the destination server
    s_new = socket.socket()
    s_new.connect((server_ip, server_port))
    data = s_new.recv(1024)
    
    delay = uniform(0, max_Delay/2)
    sleep(delay)
    print('delay = ', delay)
   
    # pass clock time recieved from time server to client
    connection.sendall(data) 
   
    # Close connection with client
    connection.close()     
    
def createDelay(max_Delay): 
  
    s = socket.socket()
    port = 10000
    s.bind(('', port)) 
    s.listen(5)       
    print("Socket is listening at port 10000...") 
        
    while True: 
        # Establish connection with client 
       connection, address = s.accept()       
       print('Proxy server connected to client ', address) 
       newClientRequest = threading.Thread(target=connectToServer, args=(connection,max_Delay, ))
       newClientRequest.start()
       
def main():
    createDelay(int(sys.argv[1]))

if __name__ == "__main__":
    main()
  
