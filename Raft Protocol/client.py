import socket 
import sys 
from time import sleep
from KThread import *
import random
import uuid

REPLY_TXN_TIMEOUT = 20

clients = ['a', 'b', 'c']
servers = ['x', 'y', 'w']
config = {'x': {'IP':'127.0.0.1', 'port':11000},
          'y': {'IP':'127.0.0.1', 'port':12000},
          'w': {'IP':'127.0.0.1', 'port':13000},
          'a': {'IP':'127.0.0.1', 'port':14000},
          'b': {'IP':'127.0.0.1', 'port':15000},
          'c': {'IP':'127.0.0.1', 'port':16000}}

lock = threading.Lock()

class Client(object):
    def __init__(self, client):
        self.name = client
        self.balance = 100
        self.sockets = {}
        self.lastLeaderHeardFrom = servers[random.randint(0,2)]
        self.txnThreads = {}
                    
    def executeTxn(self, txn_id, message, seq):
        self.sendTxnToNewLeader(txn_id, message, True, seq)
        while True:
            try:
                data = self.sockets[txn_id].recv(1024).decode()
                if len(data) == 0: 
                    self.sendTxnToSameLeader(txn_id, message, seq) 
                data = data.split('/')
                for msg in data:
                    msg = msg.split(' ')
                    if msg[0] == 'Redirect':
                        if self.lastLeaderHeardFrom == msg[1]:
                            self.sendTxnToSameLeader(txn_id, message, seq)  
                        else:
                            self.lastLeaderHeardFrom = msg[1]
                            self.sendTxnToNewLeader(txn_id, message, True, seq) 
                    elif msg[0] == 'Response' and msg[1] == 'Success':
                        print('Transaction ' + str(seq) + ' ' + 'Completed!')
                        self.balance = int(msg[2])
                        self.txnThreads[txn_id].kill()     
                    elif msg[0] == 'Response' and msg[1] == 'Fail1':
                        print('Transaction ' + str(seq) + ' ' + 'more than the balance! Aborted')
                        print("Current balance is: " + str(self.balance))
                        self.txnThreads[txn_id].kill()   
                    elif msg[0] == 'Response' and msg[1] == 'Fail2':
                        print('Transaction ' + str(seq) + ' ' + 'together with some other transaction done by you is more than the balance! Both aborted')
                        print("Current balance is: " + str(self.balance))
                        self.txnThreads[txn_id].kill()   
                    elif msg[0] == 'Balance':
                        self.balance = int(msg[1])
                        print("Current balance of " + self.name + " is: " + str(self.balance))
                        self.txnThreads[txn_id].kill()   
            except socket.timeout:
#                print('Txn' + str(seq) + ' timed out! Resent to probable Leader!\n')
                self.sendTxnToNewLeader(txn_id, message, False, seq) 
                continue
            except ConnectionResetError:
#                print('Txn' + str(seq) + ' failed as the connection to server severed! Resent to probable Leader!\n')
                self.sendTxnToNewLeader(txn_id, message, False, seq)
                continue
            
            
    def sendTxnToSameLeader(self, txn_id, message, seq):
        try:
            self.sockets[txn_id].sendall(message.encode())
        except BrokenPipeError:
            self.sendTxnToNewLeader(txn_id, message, False, seq)
            
    def sendTxnToNewLeader(self, txn_id, message, isRedirect, seq):  
        if isRedirect == False:
            self.lastLeaderHeardFrom = self.getNewProbableServer(self.lastLeaderHeardFrom)
            
        while True:
            try:
                s = socket.socket()
                s.settimeout(REPLY_TXN_TIMEOUT)
                self.sockets[txn_id] = s
                s.connect(('127.0.0.1', config[self.lastLeaderHeardFrom]['port']))
                break
            except ConnectionRefusedError:
#                print('Server ' + self.lastLeaderHeardFrom + ' is not up. Txn resent to probable Leader!\n')
                self.lastLeaderHeardFrom = self.getNewProbableServer(self.lastLeaderHeardFrom)
                sleep(2)
                continue
            
        try:
            self.sockets[txn_id].sendall(message.encode())
#            print('Txn' + str(seq) + ' sent to ' + self.lastLeaderHeardFrom)
        except BrokenPipeError:
            self.sendTxnToNewLeader(txn_id, message, False, seq)

        
    def getNewProbableServer(self, serverFailedToReply):
        temp = ['x', 'y', 'w']
        temp.remove(serverFailedToReply)
        return temp[random.randint(0,1)]
        
# python client.py client
def main():
    client = Client(sys.argv[1])
    init = 1
    if sys.argv[1] == 'init':
            filename = input("Enter filename containing initial transactions: ")
            with open(filename) as f:
                for line in f:
                    line = line.strip('\n')
                    words = line.split(' ')
                    txn = [words[0].lower(), words[1].lower(), words[2]]
                    txn_id = uuid.uuid4().hex
                    msg = 'txn' + '.' + txn_id + '.' + '*'.join(txn) + '/'
                    
                    lock.acquire()
                    client.txnThreads[txn_id] = KThread(target=client.executeTxn, args=(txn_id, msg, init,  ))
                    client.txnThreads[txn_id].start()
                    lock.release()
                    init += 1
                    sleep(0.001)
    else:
        seq = 1
        txn_id = uuid.uuid4().hex
        msg = 'bal' + '.' + txn_id + '.' + client.name + '/'
        client.txnThreads[txn_id] = KThread(target=client.executeTxn, args=(txn_id, msg, seq,  ))
        client.txnThreads[txn_id].start()
        
        sleep(1)
        while True:
            user_input = input("Enter 1 for transaction and 2 for latest balance: ")
            if user_input ==  '1':
                txn = input("Enter transaction:" )
                txn = txn.split(' ')
                
                if len(txn) ==  3 and txn[0] == client.name  and (txn[1] == 'a' or txn[1] == 'b' or txn[1] == 'c'):
                    txn_id = uuid.uuid4().hex
                    msg = 'txn' + '.' + txn_id + '.' + '*'.join(txn) + '/'
                    
                    client.txnThreads[txn_id] = KThread(target=client.executeTxn, args=(txn_id, msg, seq,  ))
                    client.txnThreads[txn_id].start()
                    seq += 1
                else:
                    print('Wrong Format! Try again')
            
            elif user_input ==  '2':
                txn_id = uuid.uuid4().hex
                msg = 'bal' + '.' + txn_id + '.' + client.name + '/'
                
                client.txnThreads[txn_id] = KThread(target=client.executeTxn, args=(txn_id, msg, seq,  ))
                client.txnThreads[txn_id].start()
                
            else:
                print('Try Again!')
            
            input("")
          
if __name__ == "__main__":
    main()