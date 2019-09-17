#!/usr/bin/python
import sys
import hashlib
import socket
import cPickle
import threading
import time
import os
import datetime

from Proxy import Proxy

# structure to hold the (ip,port,hostname,hashvalue) quadruple
class Address(object):
    def __init__(self,ip_addr,port,hostname,NODEID=None):
        self.ip_addr = ip_addr
        self.port = port
        self.hostname = hostname
        self.NODEID = NODEID

# structure to hold the (filename,hashvalue) tuple
class File(object):
    def __init__(self,filename,NODEID):
        self.filename = filename
        self.NODEID = NODEID

# main class that represents a node in the ring
class Peer(object):
    def __init__(self, address, is_root, root_addr = None):
        self.address = address
        self.is_root = is_root
        if is_root == 0:
            self.root_addr = root_addr
        self.indexfile = []
        os.system("clear")
        self.join_network()

    # initialization function
    def join_network(self):

        # start event listener thread
        t = threading.Thread(target=self.start)
        t.daemon = True
        t.start()

        # if we are root
        if self.is_root == 1:
            self.root = None
            self.successor = None
            self.sucsuccessor = None
            self.predecessor = None
            self.sec_successor = None
            self.toString()

        # if we are peer
        else:
            self.predecessor = None
            self.sucsuccessor = None

            # contact the root to find our successor
            self.root = Proxy(self.root_addr.ip_addr,self.root_addr.port)
            self.successor = self.root.find_successor(self.address.NODEID)

            # set our predecessor
            succ = Proxy(self.successor.ip_addr,self.successor.port)
            tmp = succ.getpredec()
            if not tmp:
                succ.notify(self.address)
            else:
                pred = Proxy(tmp.ip_addr,tmp.port)
                pred.revnotify(self.address)
                pred.revnotify2(self.successor)
                self.predecessor = tmp
                succ.notify(self.address)

            # get successor's successor
            self.sucsuccessor = succ.getsucc()

            # inherit necessary files from successor
            self.inherit()

            # print current state of the node
            self.toString()

        # start stabilizing thread
        s = threading.Thread(target=self.periodical)
        s.daemon = True
        s.start()

        # don't kill the main thread so that deamon threads can survive
        while True:
            time.sleep(1)

    # function to find successor
    def find_success(self,id):

        # if we are the only node in the ring
        if self.is_root == 1 and not self.successor:
            return self.address
        else:
            # if we are the successor
            if self.inbetween(id,self.address.NODEID,self.successor.NODEID):
                return self.successor
            # else ask our successor
            else:
                suc = Proxy(self.successor.ip_addr,self.successor.port)
                return suc.find_successor(id)

    # checks if node id falls between predecessor and successor
    def inbetween(self,id,small,large):
        # circular check
        if small > large:
            if id < large or id > small:
                return True
            else:
                return False
        # linear check
        else:
            if id > small and id < large:
                return True
            else:
                return False

    # inherit files from our successor if available
    def inherit(self):
        suc = Proxy(self.successor.ip_addr,self.successor.port)
        s_list = suc.getindexfile()

        # for every item in our successor
        for i in s_list:
            # if it falls into our range
            if self.inbetween(i.NODEID,self.predecessor.NODEID,self.address.NODEID):
                # add file to our list
                self.indexfile.append(i)
                print("Inherited %s from %s\n" % (i.filename, self.successor.hostname))
                # remove the file from our successor's list
                suc.removefile(i)

    # notify our successor that we are its predecessor
    def notify(self,addr):
        self.predecessor = addr
        # Only two nodes in ring
        if not self.successor:
            self.successor = addr
            suc = Proxy(addr.ip_addr,addr.port)
            suc.notify(self.address)
            self.sucsuccessor = suc.getsucc()
        self.toString()

    # notify our predecessor that we are its successor
    def revnotify(self,addr):
        self.successor = addr
        self.toString()
        pred = Proxy(self.predecessor.ip_addr,self.predecessor.port)
        pred.revnotify2(addr)

    # notify our predecessor's predecessor that we are its successor's successor
    def revnotify2(self,addr):
        self.sucsuccessor = addr
        self.toString()

    # starts the event listener thread
    def start(self):
        self.run()

    # returns the predecessor node
    def getpred(self):
        return self.predecessor

    # sets all pointers of a node to null
    def reset(self):
        self.successor = None
        self.sucsuccessor = None
        self.predecessor = None

    # ping a host to check if they are online
    def ping(self,addr):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        try:
            s.connect((addr.ip_addr, int(addr.port)))
        except socket.error:
            return False
        s.close()
        return True

    # store a file
    def store(self,file):
        self.indexfile.append(file)
        print "Successfully stored %s" % file.filename
        print "File Hash:   " + file.NODEID + "\n"
        self.toString()
        return '1'

    # recursive search for a file
    def rec_ret(self,file,filename):
        print "Looking for file %s" % filename

        # if we have the file
        if self.checkfile(file):
            print("Found!\n")
            return self.address
        # if we don't have the file
        else:
            print "I don't have file %s" % filename
            # if we have a successor, forward the search to our successor
            if self.successor:
                if self.is_root == 1:
                    print "Forwarding the search to %s...\n" % self.successor.hostname
                    suc = Proxy(self.successor.ip_addr, self.successor.port)
                    return suc.recursive(file,filename)
                else:
                    # if the search makes one full circle and comes back to the root, it means file does not exist
                    if self.successor.ip_addr == self.root_addr.ip_addr and self.successor.port == self.root_addr.port:
                        print "There is no %s in the ring...\n" % filename
                        return None
                    else:
                        print "Forwarding the search to %s...\n" % self.successor.hostname
                        suc = Proxy(self.successor.ip_addr,self.successor.port)
                        return suc.recursive(file,filename)
            # if we don't have a successor it means the file is not in the ring
            else:
                print "There is no %s in the ring...\n" % filename
                return None

    # iterative search for a file
    def iter_ret(self,file,filename):
        print "Looking for file %s" % filename
        # if we have the file
        if self.checkfile(file):
            print("Found!\n")
            return self.address
        # if we don't have the file
        else:
            print "I don't have file %s...\n" % filename
            # if we have a successor, return its address
            if self.successor:
                return self.successor
            # if we don't have a successor, it means file does not exist
            else:
                return None

    # remove a file from the indexfile
    def removefile(self,file):
        for i in range(len(self.indexfile)):
            f = self.indexfile[i]
            if f.NODEID == file.NODEID:
                del self.indexfile[i]
                return True
        return False

    # check if we have a file in our list
    def checkfile(self,file):
        for i in range(len(self.indexfile)):
            f = self.indexfile[i]
            if f.NODEID == file:
                return True
        return False

    # function that periodically runs in the background and fixes the circle in case of a node crash
    def periodical(self):
        while 1:
            # check every 3 seconds
            time.sleep(3)
            if self.successor:
                if not self.ping(self.successor):
                    # fix the broken segment by reassigning pointers
                    print("%s failed: Stabilizing...\n" % self.successor.hostname)
                    if self.successor.NODEID == self.predecessor.NODEID:
                        self.reset()
                        self.toString()
                    else:
                        self.successor = self.sucsuccessor
                        sucsuc = Proxy(self.sucsuccessor.ip_addr,self.sucsuccessor.port)
                        self.sucsuccessor = sucsuc.getsucc()
                        sucsuc.notify(self.address)
                        pred = Proxy(self.predecessor.ip_addr,self.predecessor.port)
                        pred.revnotify2(self.successor)
                        self.toString()

    # event listener
    def run(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.address.ip_addr, int(self.address.port)))
        self.sock.listen(5)

        while 1:

            # execute the necessary action regarding the incoming action code from a remote node
            try:
                conn , adr = self.sock.accept()
            except socket.error:
                print("connection error")
                break

            data = conn.recv(1024)

            # find successor
            if data == 'a':
                conn.send('1')
                id = conn.recv(1024)
                answer = self.find_success(id)
                serial_ans = cPickle.dumps(answer,-1)
                conn.send(serial_ans)

            # notify
            elif data == 'b':
                conn.send('1')
                notif_addr = conn.recv(1024)
                notif_addr = cPickle.loads(notif_addr)
                self.notify(notif_addr)

            # get predecessor
            elif data == 'c':
                pred = cPickle.dumps(self.predecessor,-1)
                conn.send(pred)

            # reverse notify
            elif data == 'd':
                conn.send('1')
                revnotif_addr = conn.recv(1024)
                revnotif_addr = cPickle.loads(revnotif_addr)
                self.revnotify(revnotif_addr)

            # reverse notify 2
            elif data == 'e':
                conn.send('1')
                revnotif_addr = conn.recv(1024)
                revnotif_addr = cPickle.loads(revnotif_addr)
                self.revnotify2(revnotif_addr)

            # get successor
            elif data == 'f':
                succ = cPickle.dumps(self.successor,-1)
                conn.send(succ)

            # CLIENT - store file
            elif data == 'g':
                conn.send('1')
                file = conn.recv(1024)
                file = cPickle.loads(file)
                res = self.store(file)
                conn.send(res)

            # remove file
            elif data == 'h':
                conn.send('1')
                file = conn.recv(1024)
                file = cPickle.loads(file)
                res = self.removefile(file)
                if res:
                    print("Removed %s from my list...\n" % file.filename)
                else:
                    print("Failed to remove %s...\n" % file.filename)
                self.toString()

            # get index file
            elif data == 'i':
                s_list = cPickle.dumps(self.indexfile,-1)
                conn.send(s_list)

            # CLIENT - recursive search
            elif data == 'j':
                conn.send('1')
                id = conn.recv(1024)
                conn.send('2')
                fname = conn.recv(1024)
                result = self.rec_ret(id,fname)
                result = cPickle.dumps(result,-1)
                conn.send(result)

            # recursive search
            elif data == 'k':
                conn.send('1')
                id = conn.recv(1024)
                conn.send('2')
                fname = conn.recv(1024)
                result = self.rec_ret(id,fname)
                result = cPickle.dumps(result,-1)
                conn.send(result)

            # CLIENT - iterative search
            elif data == 'l':
                conn.send('1')
                id = conn.recv(1024)
                conn.send('2')
                fname = conn.recv(1024)
                result = self.iter_ret(id,fname)
                result = cPickle.dumps(result, -1)
                conn.send(result)

        self.sock.close()

    # prints the current state of the node
    def toString(self):
        date= str(datetime.datetime.now())
        print("UPDATE\t\t " + date)
        print("------------")
        print("Hostname:\t %s" % (self.address.hostname + ":" + self.address.port))
        print("ID:\t\t %s" % self.address.NODEID)
        s = "-"
        p = "-"
        if self.successor:
            if self.successor.NODEID:
                s = self.successor.hostname + ":" +self.successor.port
        if self.predecessor:
            if self.predecessor.NODEID:
                p = self.predecessor.hostname + ":" + self.predecessor.port
        print("Successor:\t %s" % s)
        print("Predecessor:\t %s" % p)
        if len(self.indexfile) > 0:
            print("Files:")
            for i in self.indexfile:
                print("\t\t " + i.filename)
        else:
            print("Files:\t\t -")
        print("")

# main function
if __name__ == "__main__":
    is_root = 0
    port = None
    hostname = None
    root_port = None
    root_hostname = None

    # parse command line arguments
    for i in range(len(sys.argv)):
        if sys.argv[i] == "-m":
            is_root = int(sys.argv[i+1])
        if sys.argv[i] == "-p":
            port = sys.argv[i+1]
        if sys.argv[i] == "-h":
            hostname = sys.argv[i+1]
        if sys.argv[i] == "-r":
            root_port = sys.argv[i+1]
        if sys.argv[i] == "-R":
            root_hostname = sys.argv[i+1]

    # sanity check
    if not port or not hostname or (is_root == 0 and not (root_port and root_hostname)):
        print("Usage: <-m type> <-p own_port> <-h own_hostname> <-r root_port> <-R root_hostname>\n")
    else:
        # start the program
        print("")
        ip = socket.gethostbyname(hostname)
        # apply hash function
        nodeid = hashlib.sha1(ip).hexdigest()
        addr = Address(ip,port,hostname,nodeid)
        if is_root == 1:
            peer = Peer(addr,is_root)
        else:
            r_ip = socket.gethostbyname(root_hostname)
            root_addr = Address(r_ip,root_port,root_hostname)
            peer = Peer(addr,is_root,root_addr)
