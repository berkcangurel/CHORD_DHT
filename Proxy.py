import socket
import threading
import cPickle
import time

# proxy object to contact with a remote peer in the ring
class Proxy(object):
    def __init__(self, remote_addr, remote_port):
        self.addr = remote_addr
        self.port = remote_port
        self.mutexlock = threading.Lock()

    def find_successor(self,NODEID):
        self.mutexlock.acquire()
        code = 'a'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.addr, int(self.port)))
        self.sock.send(code)
        x = self.sock.recv(1024)
        if x == '1':
            self.sock.send(NODEID)
        answer = self.sock.recv(1024)
        answer = cPickle.loads(answer)
        self.sock.close()
        self.sock = None
        self.mutexlock.release()
        return answer

    def notify(self,addr):
        self.mutexlock.acquire()
        code = 'b'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.addr, int(self.port)))
        self.sock.send(code)
        x = self.sock.recv(1024)
        if x == '1':
            self.sock.send(cPickle.dumps(addr,-1))
        self.sock.close()
        self.sock = None
        time.sleep(1)
        self.mutexlock.release()
        return

    def getpredec(self):
        self.mutexlock.acquire()
        code = 'c'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.addr, int(self.port)))
        self.sock.send(code)
        answer = self.sock.recv(1024)
        answer = cPickle.loads(answer)
        self.sock.close()
        self.sock = None
        self.mutexlock.release()
        return answer

    def getsucc(self):
        self.mutexlock.acquire()
        code = 'f'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.addr, int(self.port)))
        self.sock.send(code)
        answer = self.sock.recv(1024)
        answer = cPickle.loads(answer)
        self.sock.close()
        self.sock = None
        self.mutexlock.release()
        return answer

    def revnotify(self,addr):
        self.mutexlock.acquire()
        code = 'd'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.addr, int(self.port)))
        self.sock.send(code)
        x = self.sock.recv(1024)
        if x == '1':
            self.sock.send(cPickle.dumps(addr,-1))
        self.sock.close()
        self.sock = None
        self.mutexlock.release()

    def revnotify2(self,addr):
        self.mutexlock.acquire()
        code = 'e'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.addr, int(self.port)))
        self.sock.send(code)
        x = self.sock.recv(1024)
        if x == '1':
            self.sock.send(cPickle.dumps(addr,-1))
        self.sock.close()
        self.sock = None
        self.mutexlock.release()

    def removefile(self,file):
        self.mutexlock.acquire()
        code = 'h'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.addr, int(self.port)))
        self.sock.send(code)
        x = self.sock.recv(1024)
        if x == '1':
            self.sock.send(cPickle.dumps(file, -1))
        self.sock.close()
        self.sock = None
        self.mutexlock.release()

    def getindexfile(self):
        self.mutexlock.acquire()
        code = 'i'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.addr, int(self.port)))
        self.sock.send(code)
        answer = self.sock.recv(1024)
        answer = cPickle.loads(answer)
        self.sock.close()
        self.sock = None
        self.mutexlock.release()
        return answer

    def recursive(self,id,filename):
        self.mutexlock.acquire()
        code = 'k'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.addr, int(self.port)))
        self.sock.send(code)
        x = self.sock.recv(1024)
        if x == '1':
            self.sock.send(id)
        x = self.sock.recv(1024)
        if x == '2':
            self.sock.send(filename)
        answer = self.sock.recv(1024)
        answer = cPickle.loads(answer)
        self.sock.close()
        self.sock = None
        self.mutexlock.release()
        return answer