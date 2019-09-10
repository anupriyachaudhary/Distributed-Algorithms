import socket
import datetime 
    
# prompt the Time Server to send time
def initiateTimeServer(): 
  
    s = socket.socket() 
    print("Socket successfully created")   
    port = 8000
    s.bind(('', port)) 
    s.listen(5)       
    print("Socket is listening at port 8000...") 
        
    
    while True:  
       # Establish connection with client 
       connection, address = s.accept()       
       print('Server connected to proxy server ', address) 
        
       # Respond the client with server clock time 
       connection.send(str(datetime.datetime.now()).encode()) 
        
       # Close the connection with the client process  
       connection.close() 


if __name__ == "__main__":
    initiateTimeServer()
 