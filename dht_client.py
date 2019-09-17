#!/usr/bin/python
import sys
import os
import hashlib
import socket
import cPickle
import threading

from dht_peer import Address, File

# client object
class dht_client(object):
    def __init__(self,addr,port,root_addr,root_port,root_hostname):
        self.addr = addr
        self.port = port
        self.root_addr = root_addr
        self.root_port = root_port
        self.root_hostname = root_hostname
        self.mutexlock = threading.Lock()
        os.system("clear")
        self.menu()

    # store a file in the ring
    def store(self,filename,id):

        file = File(filename,id)

        # contact the root and get the appropriate node to store the file based on the hash value
        code = 'a'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.root_addr, int(self.root_port)))
        self.sock.send(code)
        x = self.sock.recv(1024)
        if x == '1':
            self.sock.send(file.NODEID)
        answer = self.sock.recv(1024)
        answer = cPickle.loads(answer)
        self.sock.close()
        self.sock = None

        # contact the node directly to store the file
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((answer.ip_addr, int(answer.port)))
        code = 'g'
        self.sock.send(code)
        x = self.sock.recv(1024)
        if x == '1':
            tmp = cPickle.dumps(file,-1)
            self.sock.send(tmp)
        answer2 = self.sock.recv(1024)
        if answer2 == '1':
            print "Successfully stored %s at host %s" % (filename,answer.hostname)
            print "File Hash:   " + id + "\n"
        else:
            print "failed"
        self.sock.close()
        self.sock = None
        return

    # recursive search for a file
    def recursive(self,filename,id):
        # contact the root to search for the file and the root does all the work
        code = 'j'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.root_addr, int(self.root_port)))
        self.sock.send(code)
        x = self.sock.recv(1024)
        if x == '1':
            self.sock.send(id)
        x = self.sock.recv(1024)
        if x == '2':
            self.sock.send(filename)
        answer = self.sock.recv(1024)
        answer = cPickle.loads(answer)
        if not answer:
            print "Could not find %s ...\n" % filename
        else:
            print "Retrieved %s at %s\n" % (filename,answer.hostname)
        self.sock.close()
        self.sock = None

    # iterative search for a file
    def iterative(self,filename,id):
        code = 'l'
        stop_at_root = 0
        # contact the root to check if it has the file
        next = Address(self.root_addr,self.root_port,self.root_hostname)
        while 1:
            # if we make a full circle, that means the file does not exist
            if stop_at_root == 1:
                if next.ip_addr == self.root_addr and int(next.port) == self.root_port:
                    print "Could not find %s in the ring...\n" % filename
                    break
            stop_at_root = 1
            name = next.hostname

            # ask the node for the file
            print "Asking %s for %s" % (name,filename)
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((next.ip_addr, int(next.port)))
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
            if not answer:
                print "Could not find %s at %s" % (filename, name)
                print "Could not find %s in the ring...\n" % filename
                break
            else:
                # if the node has the file
                if answer.ip_addr == next.ip_addr and int(answer.port) == int(next.port):
                    print "Found %s at %s" % (filename,name)
                    break
                # else ask for the node's successor
                else:
                    print "Could not find %s at %s" % (filename, name)
                    next = Address(answer.ip_addr,answer.port,answer.hostname)

    # console menu
    def menu(self):
        while 1:
            print("MENU")
            print("------------")
            print("- store file (s)")
            print("- iterative retrieve (i)")
            print("- recursive retrieve (r)")
            print("- exit (e)")
            print("------------")
            choice = raw_input("Select your choice: ")

            # store file
            if  choice == "s":
                filename = raw_input("Enter filename: ")
                print("")
                # apply hash function
                id = hashlib.sha1(filename).hexdigest()
                self.store(filename,id)
                print ""

            # iterative search
            elif choice == "i":
                filename = raw_input("Enter filename: ")
                print("")
                # apply hash function
                id = hashlib.sha1(filename).hexdigest()
                self.iterative(filename, id)
                print ""

            # recursive search
            elif choice == "r":
                filename = raw_input("Enter filename: ")
                print("")
                # apply hash function
                id = hashlib.sha1(filename).hexdigest()
                self.recursive(filename, id)
                print ""

            # exit
            elif choice == "e":
                print("exiting...\n")
                return

            # invalid entry
            else:
                print("Invalid option %s\n" % choice)


if __name__ == "__main__":
    port = None
    hostname = None
    root_hostname = None
    root_port = None

    # parse command line inputs
    for i in range(len(sys.argv)):
        if sys.argv[i] == "-p":
            port = sys.argv[i+1]
        if sys.argv[i] == "-h":
            hostname = sys.argv[i+1]
        if sys.argv[i] == "-r":
            root_port = int(sys.argv[i+1])
        if sys.argv[i] == "-R":
            root_hostname = sys.argv[i+1]

    # sanity check
    if not (port and hostname and root_hostname and root_port):
        print("Usage: <-p own_port> <-h own_hostname> <-r root_port> <-R root_hostname>\n")
    else:
        # start the program
        root_ip = socket.gethostbyname(root_hostname)
        client = dht_client(hostname,port,root_ip,root_port,root_hostname)
