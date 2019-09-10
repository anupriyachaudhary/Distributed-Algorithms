import socket 
import sys 
import datetime 
from dateutil import parser 
from time import sleep
import threading
from random import randint

lock = threading.Lock()

s_in = []
s_out = []
s_snap = socket.socket()
sequence = 0
client_state = 1000
clients = ['x', 'y', 'w']
client_conf = {'x': {'IP':'127.0.0.1', 'port_in':11001, 'port_out':11002, 'port_snap':11000},
               'y': {'IP':'127.0.0.1', 'port_in':12001, 'port_out':12002, 'port_snap':12000},
               'w': {'IP':'127.0.0.1', 'port_in':13001, 'port_out':13002, 'port_snap':13000}}

snapshot_all = {}
snapshot_other = {}


# If 1st marker take snapshot (mark channel empty) and send out markers    
def respondToMarker(init, seq, client, channel):
    print("Marker <" + init + "," + str(seq) + "> receieved from node " + clients[(clients.index(client)+channel+1)%3])
    key = init + '_' +str(seq)
    if key not in snapshot_all:
        snapshot_all[key] = {'bal':0, 'initiator':(), 'c1':0, 'c2':0, 'c1_status':'open', 'c2_status':'open'}
        snapshot_all[key]['bal'] = client_state 
        snapshot_all[key]['initiator'] = (init, seq)
        marker = 'marker ' + init + ' ' + str(seq) +  '/'
        s_out[0].sendall(marker.encode())
        s_out[1].sendall(marker.encode())
    snapshot_all[key]['c'+str(channel)+'_status'] = 'closed'
    
        
def respondToMessage(money, channel, client):
    global client_state
    
    sleep(20)
    tmp = client_state + money
    lock.acquire()
    client_state = tmp
    lock.release()
    print("$" + str(money) + " recieved from " + clients[(clients.index(client)+channel-1)%3])
    print("Current balance: " + str(client_state))
    print('\n')
    for key in snapshot_all:
        if snapshot_all[key]['c'+str(channel)+'_status'] == 'open':
            snapshot_all[key]['c'+str(channel)] = snapshot_all[key]['c'+str(channel)] + money

def sendEndMessage(client):
    del_keys = []
    for key in snapshot_all:
        init = snapshot_all[key]['initiator'][0]
        seq = snapshot_all[key]['initiator'][1]
        if snapshot_all[key]['c1_status'] == 'closed' and snapshot_all[key]['c2_status'] == 'closed' and client != init :
            msg = 'end ' + str(init) + ' ' + str(seq) + ' ' + str(snapshot_all[key]['bal']) +\
                  ' ' + str(snapshot_all[key]['c1']) + ' ' + str(snapshot_all[key]['c2']) + '/'
            print('\n')
            print("END MESSAGE SENT FOR MARKER <" + init + "," + str(seq) + ">")
            del_keys.append(key)
            if init == clients[(clients.index(client)+1)%3]:
                s_out[0].sendall(msg.encode())
            else:
                s_out[1].sendall(msg.encode())
    for key in del_keys:
        lock.acquire()
        del snapshot_all[key]
        lock.release()
        
 

def respondToEndMessage(data, client, channel):
    init = data[1]
    seq = int(data[2])
    bal = data[3]
    c1 = data[4]
    c2 = data[5]
    key = init + '_' +str(seq)
    snapshot_all[key]['end'+str(channel)] = True
    print('\n')
    print("END MESSAGE RECEIVED FOR MARKER <" + init + "," + str(seq) + ">" + " FROM " +  clients[(clients.index(client)+channel-1)%3])
    key = init + '_' +str(seq)
    if key not in snapshot_other:
        snapshot_other[key] = []
    snapshot_other[key].append(clients[(clients.index(client)+channel-1)%3] + ' ' + str(bal) + ' ' + str(c1) + ' ' + str(c2))
           
def print_snapshot(client, key):
    snapshots = snapshot_other[key]
    init, seq = key.split('_')
    print("############## SNAPSHOT OF MARKER <" + init + "," + str(seq) + "> #####################")
    
    for snap in snapshots:
        values = snap.split(' ')
        print('Client_'+ values[0] + ' has state:')
        print("Balance:" + str(values[1]))
        print("From node " + clients[(clients.index(values[0])-1)%3] + ": " + str(values[2]))
        print("From node " + clients[(clients.index(values[0])-2)%3] + ": " + str(values[3]))
        print('\n')
    
    print("##########  SNAPSHOT <" + init + "," + seq + "> ENDED! ######################")
            
def endSnapshot(client):
    del_keys = []
    for key in snapshot_all:
        initiator = snapshot_all[key]['initiator'][0]
#        seq = snapshot_all[key]['initiator'][1]
        if snapshot_all[key]['c1_status'] == 'closed' and snapshot_all[key]['c2_status'] == 'closed' and \
            client == initiator and snapshot_all[key]['end1'] == True and snapshot_all[key]['end2'] == True:
            del_keys.append(key)
            snapshot_other[key].append(client + ' ' + str(snapshot_all[key]['bal']) + ' ' + str(snapshot_all[key]['c1']) + ' ' + str(snapshot_all[key]['c2']))
            print_snapshot(client, key)
    
    for key in del_keys:
        lock.acquire()
        del snapshot_all[key]
        del snapshot_other[key]
        lock.release()

def start_recv(client, channel):
    while True: 
        data = s_in[channel-1].recv(1024).decode()
        
        data = data.split('/')
        
        for msg in data:
            msg = msg.split(' ')
            if msg[0] == 'marker':
                # If marker in message
                respondToMarker(msg[1], int(msg[2]), client, channel)
                
            elif msg[0] == 'message':
                # If message save messages on non-empty incoming channels
                respondToMessage(int(msg[1]), channel, client)
            
            elif msg[0] == 'end':
                #respond to end message
                respondToEndMessage(msg, client, channel)
        
        # send message if all incoming channels closed
        sendEndMessage(client)
        
        # end snapshot algorithm
        endSnapshot(client)


def tranfer_money(client):
    global client_state
    while True:
        if randint(1,5) == 5:           # 0.2 probability of sending money
            amount = randint(1,10)
            tmpVar = client_state
            if amount <= tmpVar:
                msg = 'message ' + str(amount) + '/'
                tmpVar = client_state - amount
                lock.acquire()
                client_state = tmpVar
                lock.release()
                if randint(1,2) == 1:
                    s_out[0].send(msg.encode())
                    print("Node " + client + " sent $" + str(amount) + " to node " + clients[(clients.index(client)+1)%3])
                    print("Current balance: " + str(client_state))
                    print('\n')
                else:
                    s_out[1].send(msg.encode())
                    print("Node " + client + " sent $" + str(amount) + " to node " + clients[(clients.index(client)+2)%3])
                    print("Current balance: " + str(client_state))
                    print('\n')
        sleep(10)
        
def snap_prompt(client):
    global s_snap
    global sequence
    
    #initialize s_snap to get snapshot prompt
    s_snap = socket.socket() 
    port_snap = client_conf[client]['port_snap']
    s_snap.bind(('', port_snap))
    s_snap.listen(10)
    
    tmp = 0
    while True:  
       connection, address = s_snap.accept()  
       tmp = tmp+1
       lock.acquire()
       sequence = tmp
       lock.release()
       #print("Current sequence is: " + str(sequence))
       
       key = client+ '_'+ str(sequence)
       snapshot_all[key] = {'bal':0, 'initiator':(), 'c1':0, 'c2':0, 'c1_status':'open', 'c2_status':'open', 'end1':False, 'end2':False}
       # Take snapshot
       lock.acquire()
       snapshot_all[key]['bal'] = client_state
       snapshot_all[key]['initiator'] = (client, sequence)
      
       # Send markers on outgoing channels
       marker = 'marker ' + client + ' ' + str(sequence) +  '/'
       s_out[0].sendall(marker.encode())
       s_out[1].sendall(marker.encode())
       print("<<<<<<<<< SNAPSHOT HAS BEEN INITIATED! >>>>>>>>>>")
       print("MARKER <" + client + "," + str(sequence) + "> SENT TO NODE " +  clients[(clients.index(client)+1)%3] + " and " + clients[(clients.index(client)+2)%3])
       print('\n')
       lock.release()
       connection.close() 
       
def startServer(client):
    # create incoming socket
    s_i = socket.socket()
    s_i.bind(('127.0.0.1', client_conf[client]['port_in']))
    s_i.listen()
    while True:  
       connection, address = s_i.accept()   
       s_in.append(connection)
                
    
def init_client(client):
#    init_synchronizeTime()
    # initialize sockets to connect to other clients
    
    # create socket catering to outgoing  messages
    s_1 = socket.socket()
    s_2 = socket.socket()
    s_out.append(s_1)
    s_out.append(s_2)

    t_server = threading.Thread(target=startServer, args=(client, ))
    t_server.start()
    sleep(10)
    
    # connect X->Y&W, Y->W&X, W->X&Y
    s_1.connect(('127.0.0.1', client_conf[clients[(clients.index(client)+1)%3]]['port_in']))
    print('Node ' + client + ' connected to ' + clients[(clients.index(client)+1)%3])
    sleep(10)
    s_2.connect(('127.0.0.1', client_conf[clients[(clients.index(client)+2)%3]]['port_in']))
    print('Node ' + client + ' connected to ' + clients[(clients.index(client)+2)%3])
    print('\n')
       
    
    sleep(10)
    
    t_start_channel1 = threading.Thread(target=start_recv, args=(client, 1, )) 
    t_start_channel1.start()
    
    t_start_channel2 = threading.Thread(target=start_recv, args=(client, 2, )) 
    t_start_channel2.start()
    
    
    t_money = threading.Thread(target=tranfer_money, args=(client, ))
    t_money.start()
    
    

# python client.py client 
def main():
    print('start!')
    client = sys.argv[1]
     # creating thread 
     

    t_init = threading.Thread(target=init_client, args=(client, )) 
    t_init.start()
    
    t_prompt = threading.Thread(target=snap_prompt, args=(client, )) 
    t_prompt.start()
    

             
if __name__ == "__main__":
    main()  
    

  

    
  
