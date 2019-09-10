import socket 
import sys 
import datetime 
from time import sleep
import threading
import random
import queue
from hashlib import sha256
from KThread import *
import copy

T = 7   #broadcast time
REPLY_VOTE_TIMEOUT = 2
HEARTBEAT_TIMEOUT = 2
INITIAL_BALANCE = 100

servers = ['x', 'y', 'w']
server_conf = {'x': {'IP':'127.0.0.1', 'port':11001, 'port_client':11000},
               'y': {'IP':'127.0.0.1', 'port':12001, 'port_client':12000},
               'w': {'IP':'127.0.0.1', 'port':13001, 'port_client':13000}}

lock = threading.Lock()
lock1 = threading.Lock()
lock2 = threading.Lock()
lock3 = threading.Lock()

class Block(object):
    def __init__(self, index, term, hashPreviousBlock, merkelTreeHash, nonce, MTnonceHash, transactions, balance):
        self.index = index 
        self.term = term
        self.hashPrevBlock = hashPreviousBlock
        self.MThash = merkelTreeHash
        self.nonce = nonce
        self.MTnonceHash = MTnonceHash
        self.txns = transactions
        self.balance = balance

class Server(object):
    def __init__(self, server):
        self.server = server
        self.transactionPool = queue.Queue(maxsize=50)
        self.transactionIDs = []
        self.s = {}
        self.role = 'follower'
        self.peers = ['x', 'y', 'w']
        self.peers.remove(server)
        self.probableLeader = server
        
        # TODO: read log from stable storage         
        self.log = []
        self.commitIndex = -1   
        self.currentTerm = -1
        self.votedFor = {}
        
        self.resetVariables()
                     
        # initialize sockets to connect to other servers
        threading.Thread(target=self.listenToServers, args=()).start()
        sleep(10)
        s_2 = socket.socket()
        connToPeer = servers[(servers.index(server)+1)%3]
        self.s[connToPeer] = s_2
        s_2.connect(('127.0.0.1', server_conf[servers[(servers.index(server)+1)%3]]['port']))
        
        self.connectToServer = threading.Thread(target=self.connToServers, args=())
           
        sleep(5)
        print('Servers are connected!')
        print('\n')
        
        #  start a thread to connect to client
        # maintain logs of connenction info for each txn for different transactions to reply them back
        self.txnConnectionLog = {}
        threading.Thread(target=self.connect_client, args=()).start()
        # create a thread to do proof of work by taking two txn from transactionPool
        threading.Thread(target=self.proofOfWork, args=()).start()
        
        # initialize threads
        self.main = threading.Thread(target=self.main, args=())
        self.main.start()
        self.timer = threading.Thread(target=self.timer, args=())
        self.timer.start()
        
        # initialize seperate thread to repond to incoming messages from peers
        for peer in self.peers:
            threading.Thread(target=self.respond_RPC, args=(peer, )).start()
            
            
    def resetVariables(self):
        # initialize/reset variables for follower state
        self.votedFor = {}
        self.last_update = datetime.datetime.now()
        
        # initialize/reset variables for candidate state 
        self.votesFrom = []
        self.numOfVotes  = 0
        self.requestVoteTimer = datetime.datetime.now()
        
        # initialize/reset variables for leader state 
        self.heartbeatTimer = datetime.datetime.now()
        self.nextIndex = {}
        self.matchIndex = {}
        for peer in self.peers:
            self.nextIndex[peer] = len(self.log)
            self.matchIndex[peer] = len(self.log)
            
        
    def listenToServers(self):
        # create incoming socket
        s_1 = socket.socket()
        s_1.bind(('127.0.0.1', server_conf[self.server]['port']))
        s_1.listen()
        while True:  
           connection, address = s_1.accept() 
           connToPeer = servers[(servers.index(self.server)+2)%3]
           self.s[connToPeer] = connection
           
    def connToServers(self):
        # connect X->Y, Y->W, W->X
        while True:
            try:
                s_2 = socket.socket()
                connToPeer = servers[(servers.index(self.server)+1)%3]
                self.s[connToPeer] = s_2
                s_2.connect(('127.0.0.1', server_conf[servers[(servers.index(self.server)+1)%3]]['port']))
                print(self.server + ' connected to ' + connToPeer)
                break
            except ConnectionRefusedError:
                sleep(1)
                continue

           
    def connect_client(self):
        s = socket.socket() 
        port_client = server_conf[self.server]['port_client']
        s.bind(('', port_client))
        s.listen(50)
        self.txn_thread = {}
        while True:  
           connection, address = s.accept()
           #  start thread to respond to client
           self.txn_thread[address] = KThread(target=self.getTxnData, args=(address, ))
           lock3.acquire()
           self.txnConnectionLog[address] = connection
           lock3.release()
           self.txn_thread[address].start()
           
           
    def getTxnData(self, address):
        while True: 
           connection = self.txnConnectionLog[address]
           data = connection.recv(1024).decode()
           if self.role == 'leader':
               data = data.split('/')
               for msg in data:
                   if msg == '':
                       continue
                   msg = msg.split('.')
                   uuid =  msg[0] 
                   add = str(address[0]) +'?'+ str(address[1]) 
                   txnData = uuid + '+' + add + '+' + msg[1]
                   if uuid not in self.transactionIDs:
                       lock.acquire()
                       self.transactionIDs.append(uuid)
                       self.transactionPool.put(txnData)
                       lock.release()
#                       print(list(self.transactionPool.queue))
                       threadToKill = self.txn_thread.pop(address, None) 
                       threadToKill.kill() 
           else:
               message = 'Redirect ' + self.probableLeader + '/'
               connection.sendall(message.encode())
               threadToKill = self.txn_thread.pop(address, None) 
               threadToKill.kill() 
        
           
    def sendMessageToPeer(self, peer, message):
        try:
            self.s[peer].sendall(message.encode())
        except BrokenPipeError:
            return
                
           
    def main(self):
        self.isStateReset = True
        while True:
            if self.role == 'follower':
                self.follower()
            elif self.role == 'candidate':
                self.candidate()
            elif self.role == 'leader':
                self.leader()
            sleep(0.01)
            
                       
    def timer(self):
        while True:
            election_timeout = T * random.random() + T
            while (datetime.datetime.now() - self.last_update).total_seconds() <= election_timeout:
                sleep(0.01)
                pass
            if self.role == 'follower' or self.role == 'candidate':
                self.isStateReset = True
                if self.role == 'follower':
                    self.role = 'candidate'
                    
                    
    def follower(self):
        # initialize variables for follower state
        if self.isStateReset == True:
            self.role == 'follower'
            self.resetVariables()
            self.isStateReset = False
        
        
    def candidate(self):
        # initialize variables for candidate state
        if self.isStateReset == True:
            self.resetVariables()
            
            self.currentTerm += 1
            print('Election for term ' +  str(self.currentTerm) + ' Begins!')
            self.votesFrom.append(self.server)
            self.votedFor[self.currentTerm] = self.server
            self.numOfVotes = 1
         
        if (self.isStateReset == True or (datetime.datetime.now() - self.requestVoteTimer).total_seconds() > REPLY_VOTE_TIMEOUT):
            for peer in self.peers:
                if peer not in self.votesFrom:
                    message = 'RequestVote ' + str(self.currentTerm) + ' ' + self.server + ' ' + \
                                str(self.getLastLogTerm()) + ' ' + str(self.getLastLogIndex()) + '/'
                    self.sendMessageToPeer(peer, message)
            self.requestVoteTimer = datetime.datetime.now()
            self.isStateReset = False
                       
            
    def leader(self):
        if self.isStateReset == True:
            self.resetVariables()
            print('I am the Leader!')
            
        if (self.isStateReset == True or (datetime.datetime.now() - self.heartbeatTimer).total_seconds() > HEARTBEAT_TIMEOUT):
            self.sendHeartbeat()
            self.heartbeatTimer = datetime.datetime.now()
            self.isStateReset = False
            
            
    def sendHeartbeat(self):
        for peer in self.peers:
            prevLogTerm = self.getPrevLogTerm(peer)
            prevLogIndex = self.getPrevLogIndex(peer)
            message = 'AppendEntries ' + str(self.currentTerm) + ' ' + self.server + ' ' + \
                        str(prevLogTerm) + ' ' + str(prevLogIndex) + \
                        ' Empty ' + str(self.commitIndex) + '/'
            self.sendMessageToPeer(peer, message)
            
               
    def proofOfWork(self):
        # 1. check if log doesnt have the same transaction stored
        # 2. check if transaction doesn't conflict with previous transaction i.e. sending money > balance
        # 3. mine the block i.e. find a nonce that results in SHA256(block) < Difficulty
        while True:
            if self.transactionPool.qsize() >= 2:
                txnsData = []
                uuids = []
                addresses = []
                txns = []
                clients = []
                
                lock1.acquire()
                if self.transactionPool.qsize() < 2:
                    lock1.release()
                    continue
                txnsData.append(self.transactionPool.get())
                txnsData.append(self.transactionPool.get())
#                print(list(self.transactionPool.queue))
                lock1.release()
                
                for txnData in txnsData:
                    temp = txnData.split('+')
                    
                    uuids.append(temp[0])
                    ip, port = temp[1].split('?')
                    addresses.append((ip, int(port)))
                    txn = temp[2].split('*')
                    txns.append(txn[0] + '-' + txn[1] + '-' + txn[2])
                    clients.append(txn[0])
                
                logSizeBeforePoWStarted = len(self.log)
                if len(txns) == 2 and self.isTxnsAppendable(txns, uuids, clients, addresses) == True and self.role == 'leader':
                    nonce = self.mineBlock(txns)
                    block = self.createBlock(txns, uuids, nonce)
                    self.appendBlockInLog(block, uuids, clients, addresses, logSizeBeforePoWStarted)
            sleep(0.001)
            
            
    def isTxnsAppendable(self, txns, uuids, clients, addresses):
        isTxn1AlreadyInLog, isTxn2AlreadyInLog = self.isTxnsAlreadyInLog(uuids, clients, addresses)
        if isTxn1AlreadyInLog == True or isTxn2AlreadyInLog == True:
            return False
        
        isTogetherConflict, isTxn1Conflict, isTxn2Conflict = self.isTxnsConflictWithPreviousEntries(clients, txns, addresses)
        if isTxn1Conflict == True or isTxn2Conflict == True or isTogetherConflict ==  True:
            return False
        
        return True
        
                                
    def isTxnsAlreadyInLog(self, uuids, clients, addresses):
        isTxn1AlreadyInLog = False
        isTxn2AlreadyInLog = False
        if len(self.log) == 0:
            return (isTxn1AlreadyInLog, isTxn2AlreadyInLog)
        
        for entry in reversed(self.log):
            for txn in entry.txns:
                if txn[0] == uuids[0]:
                    isTxn1AlreadyInLog = True
                    if entry.index <= self.commitIndex:
                        self.replyClient(clients[0], addresses[0], 'Success')
                    if isTxn1AlreadyInLog == True  and isTxn2AlreadyInLog == True:
                        return (isTxn1AlreadyInLog, isTxn2AlreadyInLog)
                elif txn[0] == uuids[1]:
                    isTxn2AlreadyInLog = True
                    if entry.index <= self.commitIndex:
                        self.replyClient(clients[1], addresses[1], 'Success')
                    if isTxn1AlreadyInLog == True  and isTxn2AlreadyInLog == True:
                        return (isTxn1AlreadyInLog, isTxn2AlreadyInLog)
        return (isTxn1AlreadyInLog, isTxn2AlreadyInLog)
                    
            
    def isTxnsConflictWithPreviousEntries(self, clients, transactions, addresses):
        isTxn1Conflicting = False
        isTxn2Conflicting = False
        isTogetherConflicting = False
        
        moneyToBeTransferred = []
        balance = []
        for i in range(0,2):
            temp = transactions[i].split('-')
            moneyToBeTransferred.append(int(temp[2]))
            if len(self.log) == 0:
                balance.append(INITIAL_BALANCE)
            else:
                balance.append(self.log[len(self.log)-1].balance[clients[i]])
        
        if moneyToBeTransferred[0] > balance[0]:
            self.replyClient(clients[0], addresses[0], 'Fail1')
            isTxn1Conflicting = True
        if moneyToBeTransferred[1] > balance[1]:
            self.replyClient(clients[1], addresses[1], 'Fail1')
            isTxn2Conflicting = True
            
        if clients[0] == clients[1] and (moneyToBeTransferred[0] + moneyToBeTransferred[1]) > balance[0]:
            if isTxn1Conflicting == False and isTxn2Conflicting == False:
                self.replyClient(clients[0], addresses[0], 'Fail2')
                self.replyClient(clients[1], addresses[1], 'Fail2')
            isTogetherConflicting = True
        
        return (isTogetherConflicting, isTxn1Conflicting, isTxn2Conflicting)
        
        
    def replyClient(self, client, address, result):
        if self.commitIndex == -1:
            balance = INITIAL_BALANCE
        else:
            balance = self.log[self.commitIndex].balance[client]
        message = 'Response ' + result + ' ' + str(balance) + '/'
        
        self.txnConnectionLog[address].sendall(message.encode())
        
        sleep(5)
        self.txnConnectionLog[address].close()
        self.txnConnectionLog.pop(address, None) 
        
        
    def mineBlock(self, transactions):
        concatTxnHash = sha256(transactions[0].encode()).hexdigest() + sha256(transactions[1].encode()).hexdigest()
        MThash = sha256(concatTxnHash.encode()).hexdigest()
            
        while True:
            nonce  = ''.join([str(random.randint(0, 9)) for i in range(8)])
            
            concatMTnonce = sha256((MThash + nonce).encode()).hexdigest()
            lastChar = concatMTnonce[-1]
            if lastChar == '0' or lastChar == '1' or lastChar == '2':
                break
            
        return nonce
        
    def createBlock(self, txns, uuids, nonce):
        index = len(self.log)
        term = self.currentTerm
        
        if index == 0:
            hashPrevBlock = ''
        else:
            prevBlock = self.log[index-1]
            headerOfPrevBlock = str(prevBlock.term) + str(prevBlock.hashPrevBlock) + str(prevBlock.MThash) + str(prevBlock.nonce)
            hashPrevBlock = sha256(headerOfPrevBlock.encode()).hexdigest()
        
        concHash = sha256(txns[0].encode()).hexdigest() + sha256(txns[1].encode()).hexdigest()
        MThash = sha256(concHash.encode()).hexdigest()
        
        MTnonceHash = sha256((MThash + nonce).encode()).hexdigest()
        
        transactions = [(uuids[0], txns[0]), (uuids[1], txns[1])]
        
        if index == 0:
            balance = {'a': INITIAL_BALANCE, 'b': INITIAL_BALANCE, 'c': INITIAL_BALANCE}
        else:
            balance = copy.deepcopy(prevBlock.balance)
        for txn in txns:
            txn  = txn.split('-')
            fromClient = txn[0]
            toClient = txn[1]
            moneyTransferred = int(txn[2])
            
            balance[fromClient] = balance[fromClient] - moneyTransferred
            balance[toClient] = balance[toClient] + moneyTransferred
        
        return Block(index, term, hashPrevBlock, MThash, nonce, MTnonceHash, transactions, balance)
        
        
    def appendBlockInLog(self, block, uuids, clients, addresses, logSizeBeforePoWStarted):
        if self.role == 'leader':
            lock2.acquire()
            if logSizeBeforePoWStarted != len(self.log):
                lock2.release()
                return
            self.log.append(block) 
            lock2.release()
            self.transactionIDs.remove(uuids[0])
            self.transactionIDs.remove(uuids[1])
            
            self.commitLogs(block)
            self.replyClient(clients[0], addresses[0], 'Success')
            self.replyClient(clients[1], addresses[1], 'Success')
            
                
    def commitLogs(self, block):
        for peer in self.peers:
            self.sendAppendEntries(peer)
        while True:
            if self.role == 'leader':
                if self.ifBlockReplicatedOnMajority(block.index) and self.isPresentTermOneEntryOnMajority():
                    if self.commitIndex < block.index:
                        self.commitIndex = block.index
                    return True
            sleep(0.1)
            
            
    def ifBlockReplicatedOnMajority(self, block_index):
        no_replication = 0
        majority = len(self.peers)/2
        for peer in self.peers:
            if self.matchIndex[peer] >= block_index:
                no_replication += 1
        if no_replication >= majority:
            return True
        return False
    
            
    def isPresentTermOneEntryOnMajority(self):
        term = self.currentTerm
        for entry in reversed(self.log):
            if entry.term < term:
                return False
            if entry.term == term and self.ifBlockReplicatedOnMajority(entry.index):
                return True
        return False
                
            
    def sendAppendEntries(self, peer):
        lastLogIndex = self.getLastLogIndex()
        nextIndex = self.nextIndex[peer]
        if lastLogIndex >= nextIndex:   
            prevLogTerm = self.getPrevLogTerm(peer)
            prevLogIndex = self.getPrevLogIndex(peer)
            entries = ''
            for index in range(nextIndex, len(self.log)):
                block = self.log[index] 
                txn1 = str(block.txns[0][0]) + '$' + str(block.txns[0][1])
                txn2 = str(block.txns[1][0]) + '$' + str(block.txns[1][1])
                entries += str(block.index) + ',' + str(block.term) + ',' + str(block.hashPrevBlock) + ',' + \
                            str(block.MThash) + ',' + str(block.nonce) + ',' + str(block.MTnonceHash) + ',' + \
                            str(txn1) + ',' + str(txn2) + ',' + \
                            str(block.balance['a']) + ',' + str(block.balance['b']) + ',' + str(block.balance['c']) + '#'
            message = 'AppendEntries ' + str(self.currentTerm) + ' ' + self.server + ' ' + \
                        str(prevLogTerm) + ' ' + str(prevLogIndex) + ' ' + \
                        entries + ' ' + str(self.commitIndex) + '/'
            self.sendMessageToPeer(peer, message)
                           
                        
    def respond_RPC(self, peer):
        while True: 
            try:
                data = self.s[peer].recv(4096).decode()
            except ConnectionResetError:
                self.connectToServer.start()
                self.connectToServer.join()
                continue
            data = data.split('/')
            for elem in data:
                msg = elem.split(' ')
                if msg[0] == 'GivenVote':
                    self.respondToGivenVote(int(msg[1]), msg[2]) 
                elif msg[0] == 'RequestVote':
                    self.respondToRequestVote(int(msg[1]), msg[2], int(msg[3]), int(msg[4]), peer) 
                elif msg[0] == 'AppendEntries':
                    self.respondToAppendEntries(int(msg[1]), msg[2], int(msg[3]), int(msg[4]), msg[5], int(msg[6]))
                elif msg[0] == 'AcceptAppendEntries':
                    self.respondToReplyToAppendEntries(int(msg[1]), msg[2], msg[3])
                    
    
    def respondToGivenVote(self, term, voterId):
        if term > self.currentTerm:
            self.currentTerm = term
            if self.role == 'leader' or self.role == 'candidate':
                self.stepDown()
        elif term == self.currentTerm and self.role == 'candidate':
            self.numOfVotes += 1
            self.votesFrom.append(voterId)
            if self.numOfVotes > len(servers)/2:
                self.becomeLeader()
                    
    def respondToRequestVote(self, term, candidateId, lastLogIndex, lastLogTerm, peer):
        if term > self.currentTerm:
            self.currentTerm = term
            if self.role == 'leader' or self.role == 'candidate':
                self.stepDown()
        if term == self.currentTerm and self.isNotYetVotedForCandidate(term, candidateId):
            if self.getLastLogTerm() > lastLogTerm or (self.getLastLogTerm() == lastLogTerm and self.getLastLogIndex() > lastLogIndex):
                return
            else:
                self.votedFor[term] = candidateId
                message = 'GivenVote ' + str(self.currentTerm) + ' ' + self.server + '/'
                self.sendMessageToPeer(peer, message)
                self.last_update = datetime.datetime.now()
                
    def isNotYetVotedForCandidate(self, term, candidateId):    
        if term not in self.votedFor:
            return True
        elif term in self.votedFor: 
            if candidateId != self.votedFor[term]:
                return True
        else:
            return False
                    
    def respondToAppendEntries(self, term, leaderId, prevLogTerm, prevLogIndex, entries, commitIndex):
        if  term < self.currentTerm:
            return
        if term > self.currentTerm:
            self.currentTerm = term
            if self.role == 'leader' or self.role == 'candidate':
                self.stepDown()
        
        self.probableLeader = leaderId
        self.last_update = datetime.datetime.now()
        
        # If hearbeat
        if entries == 'Empty':
            return
        # if not heartbeat
#        elif len(self.log) < prevLogIndex+1:
#            message = 'AcceptAppendEntries ' + str(self.currentTerm) + ' ' + self.server + ' ' + str('Fail') + '/'
#            self.s[leaderId].sendall(message.encode())  
        elif (len(self.log) == 0 and prevLogIndex == -1) or (len(self.log) > prevLogIndex 
                                                              and self.log[prevLogIndex].term == prevLogTerm):
            entries = entries.split('#')
            index = prevLogIndex+1
            for entry in entries:
                if entry == '':
                    break
                entry = entry.split(',')
                t1 = entry[6].split('$')
                t2 = entry[7].split('$')
                block = Block(int(entry[0]), int(entry[1]), entry[2], entry[3], int(entry[4]), entry[5], [(t1[0],t1[1]), (t2[0],t2[1])],\
                              {'a':int(entry[8]), 'b':int(entry[9]), 'c':int(entry[10])})
                if len(self.log) > index:
                    self.log[index] = block
                else:
                    self.log.append(block)
                index += 1
            self.commitIndex = commitIndex
            message = 'AcceptAppendEntries ' + str(self.currentTerm) + ' ' + self.server + ' ' + str('Success') + '/'
            self.s[leaderId].sendall(message.encode())       
        else:
            message = 'AcceptAppendEntries ' + str(self.currentTerm) + ' ' + self.server + ' ' + str('Fail') + '/'
            self.s[leaderId].sendall(message.encode())
                
            
    def respondToReplyToAppendEntries(self, term, server, result):
        if term > self.currentTerm:
            self.currentTerm = term
            if self.role == 'leader' or self.role == 'candidate':
                self.stepDown()
        elif term == self.currentTerm and result == 'Success':
            self.nextIndex[server] = len(self.log)
            self.matchIndex[server] = len(self.log)-1
        elif term == self.currentTerm and result == 'Fail':
            if len(self.log) != 0:
                lock.acquire()
                self.nextIndex[server] -= 1
                lock.release()
                self.sendAppendEntries(server)
            
    def stepDown(self):
        self.isStateReset = True
        self.role = 'follower'
        
    def becomeLeader(self):
        self.isStateReset = True
        self.role = 'leader'
        
    def getLastLogTerm(self):
        if len(self.log) == 0:
            return -1
        return self.log[len(self.log)-1].term
        
    def getLastLogIndex(self):
        if len(self.log) == 0:
            return -1
        return self.log[len(self.log)-1].index
     
    def getPrevLogTerm(self, peer):
        nextIndex = self.nextIndex[peer]
        if len(self.log) == 0 or nextIndex == 0:
            return -1
        return self.log[nextIndex-1].term
         
    def getPrevLogIndex(self, peer):
        nextIndex = self.nextIndex[peer]
        if len(self.log) == 0 or nextIndex == 0:
            return -1
        return self.log[nextIndex-1].index
    
    def printBlockChain(self):
        s = ' '
        h = '#'
        l = '|'
        d = '-'
        for index in range(0, len(self.log)):
            print(str(index))
            block = self.log[index]
            print(80*h)
            print(1*l + 35*s + 'Term = '+ str(block.term) + 35*s + 1*l)
            if index == 0:
                print(1*l + 78*s +1*l)
            else:
                print(1*l + 7*s + block.hashPrevBlock + 7*s + 1*l)
            print(1*l + 5*s + 'MT = '+  block.MThash + 4*s + 1*l)
            print(1*l + 31*s + 'Nonce = '+ str(block.nonce) + 31*s + 1*l)
            for txn in block.txns:
                txn = txn[1].split('-')
                t = ' '.join(txn)
                if int(txn[2]) < 10: 
                    print(1*l + 36*s + t + 35*s + 1*l)
                else:
                    print(1*l + 36*s + t + 34*s + 1*l)
            print(1*l + 78*d + 1*l)
            print(1*l + 28*s + 'SHA56 of MT and nonce' +29*s + 1*l)
            print(1*l + 7*s + block.MTnonceHash + 7*s + 1*l)
            print(80*h)
            for i in (0, 5):
                print(37*s + '||')
            print(37*s + '\/')
        print('\n\n\n')
        
             
# python server.py server 
def main():
    print('Initialize sever ' + sys.argv[1])
    server = Server(sys.argv[1])
    
    sleep(15)
    while True:
        input("\n\nPress enter to print the blockchain: \n\n" )
        server.printBlockChain()
        input("")
          
if __name__ == "__main__":
    main()  
    