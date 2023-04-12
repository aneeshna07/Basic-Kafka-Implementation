import socket
import time

host = '127.0.0.1'
port = 2004
print('Waiting for connection response')
all_brokers=[2005,2006,2007]

def update_brokerlist(ls):
    global all_brokers
    all_brokers=[x for x in all_brokers if x not in ls]
    while True:
        ClientMultiSocket1 = socket.socket()
        try:
            ClientMultiSocket1.connect((host, port))
        except socket.error as e:
            print(e)
        else:
            recv=ClientMultiSocket1.recv(1024)
            print(recv.decode())
            ClientMultiSocket1.send(str.encode("3"))
            ClientMultiSocket1.send(str.encode(str(all_brokers)))
            try:
                res=ClientMultiSocket1.recv(1024)
                print(res.decode())
            except Exception as e:
                print(e)
            else:
                break

while True:    
    l=list()
    for i in range(len(all_brokers)):
        ClientMultiSocket = socket.socket()
        try:
            ClientMultiSocket.connect((host, all_brokers[i]))
        except socket.error as e:
            time.sleep(10)
            try:
                ClientMultiSocket.connect((host, all_brokers[i]))
            except:
                l.append(all_brokers[i])
            else:
                rec=ClientMultiSocket.recv(1024)
                ClientMultiSocket.close()
        else:
            rec=ClientMultiSocket.recv(1024)
            ClientMultiSocket.close()
    if len(l)!=0:
        update_brokerlist(l)
    time.sleep(5)
    