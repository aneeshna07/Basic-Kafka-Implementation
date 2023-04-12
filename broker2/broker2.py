import socket
import os
from _thread import *
import time

ServerSideSocket = socket.socket()
host = '127.0.0.1'
port = 2007
ThreadCount = 0
no_reads={}
no_writes={}

try:
    ServerSideSocket.bind((host, port))
except socket.error as e:
    print(str(e))
print('Socket is listening..')
ServerSideSocket.listen()

def log_file_dump(port, log_message):
    f = open('config.txt', 'a')
    f_1 = open('../broker1/config.txt', 'a')
    f_2 = open('../broker2/config.txt', 'a')

    f.write('\n')
    f.write(f' {time.strftime("%H:%M:%S", time.localtime())}')
    f.write(f'\t{port} : {log_message}')

    f_1.write('\n')
    f_1.write(f' {time.strftime("%H:%M:%S", time.localtime())}')
    f_1.write(f'\t{port} : {log_message}')

    f_2.write('\n')
    f_2.write(f' {time.strftime("%H:%M:%S", time.localtime())}')
    f_2.write(f'\t{port} : {log_message}')

    f.close()
    f_1.close()
    f_2.close()

def producer(connection,topic):
    global no_reads
    global no_writes
    not_replicate={}
    not_replicate[topic]=list()
    connection.send(str.encode("Send File"))
    try:
        l=len(no_writes[topic])
    except KeyError:
        no_writes[topic]=list()
    while len(no_writes[topic])!=0:
        pass
    time.sleep(2)
    try:
        no_reads[topic].append(port)
    except KeyError:
        no_reads[topic]=list()
        no_reads[topic].append(port)
    try:
        file=connection.recv(1024)
    except Exception as e:
        print(e)
        connection.close()
        return
    else:
        file=file.decode()
        print(file)
        #partitioning function
        try:
            connection.send(str.encode("send all brokers"))
            all_brokers=connection.recv(1024)
            
        except Exception as e:
            print(e)
            connection.close()
            return
        else:
            all_brokers=all_brokers.decode()
            print(all_brokers)
            all_brokers=list(map(int,all_brokers.strip('[').strip(']').split(',')))
            all_brokers=[x for x in all_brokers if x!=port]
            for i in range(len(all_brokers)):
                followerSocket=socket.socket()
                try:
                    followerSocket.connect((host, all_brokers[i]))
                except socket.error as e:
                    print("Couldn't replicate with this broker connected to"+str(all_brokers[i])+", moving on")
                    not_replicate[topic].append(str(all_brokers[i]))
                    print(not_replicate[topic])
                else:
                    rec1 = followerSocket.recv(1024)
                    print(rec1.decode())
                    followerSocket.send(str.encode("3"))
                    rec2=followerSocket.recv(1024)
                    print(rec2.decode())
                    followerSocket.send(topic.encode())
                    try:
                        rec2=followerSocket.recv(1024)
                        followerSocket.send(file.encode())
                    except Exception as e:
                        print("Couldn't replicate with this broker connected to"+str(all_brokers[i])+", moving on")
                        not_replicate[topic].append(str(all_brokers[i]))
                    else:
                        try:
                            rec3=followerSocket.recv(1024)
                        except Exception as e:
                            print("Couldn't replicate with this broker connected to"+str(all_brokers[i])+", moving on")
                            not_replicate[topic].append(str(all_brokers[i]))
                        else:
                            followerSocket.close()
            if i==(len(all_brokers)-1):
                print("Auto-replication failed for the following ports, please replicate manually: ")
                for i in range(len(not_replicate[topic])):
                    print(not_replicate[topic][i])
            connection.send(str.encode("Task complete!"))
            connection.close()
            no_reads[topic].remove(port)

def follower(connection):
    global no_reads
    global no_writes
    connection.send(str.encode("Connection successful, send Topic!"))
    try:
        topic=connection.recv(1024).decode
    except Exception as e:
        print("Connection unsuccessful please replicate manually")
    else:
        connection.send(str.encode("Topic received, send file!"))
        try:
            file=connection.recv(1024).decode
        except Exception as e:
            print("Connection unsuccessful please replicate manually")
        else:
            try:
                l=len(no_reads[topic])
            except KeyError:
                no_reads[topic]=list()
            while len(no_reads[topic])!=0:
                pass
            try:
                l=len(no_writes[topic])
            except KeyError:
                no_writes[topic]=list()
            while len(no_writes[topic])!=0:
                pass
            no_writes[topic].append(port)
            #replicate()
            connection.send(str.encode("Replication successful"))
            connection.close()
    no_writes[topic].remove(port)

def consumer(connection):
    global no_writes
    global no_reads
    
    try:
        l=connection.recv(1024)
    except Exception as e:
        connection.close()
        print(e)
    else:
        l=l.decode().split(',')
        print(l)
        topic=l[1]
        flag=l[0]
        try:
            l=len(no_reads[topic])
        except KeyError:
            no_reads[topic]=list()
        while len(no_reads[topic])!=0:
            pass
        try:
            l=len(no_writes[topic])
        except KeyError:
            no_writes[topic]=list()
        while len(no_writes[topic])!=0:
            pass
        no_writes[topic].append(port)
        if flag=="1":
            file="hi123g"#get_from_beg(topic)
        else:
            file="hi"#get_latest(topic)
        connection.send(file.encode())
        connection.close()
        no_writes[topic].remove(port)
    
while True:
    try:
        Client, address = ServerSideSocket.accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        Client.send(str.encode("Server working"))
        recv=Client.recv(1024)
        recv=recv.decode()
        if recv=='1':
            topic=Client.recv(1024)
            topic=topic.decode()
            producer(Client,topic)
        elif recv=='2':
            consumer(Client)
        elif recv=="3":
            follower(Client)
        elif recv=="4":
            Client.send(str.encode("breathing"))
            Client.close()
        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))
    except socket.error as e:
        print(str(e))
ServerSideSocket.close()