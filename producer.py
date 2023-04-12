import socket
ClientMultiSocket = socket.socket()
host = '127.0.0.1'
port = 2004
print('Waiting for connection response')
try:
    ClientMultiSocket.connect((host, port))
except socket.error as e:
    print(str(e))
res = ClientMultiSocket.recv(1024)
print(res.decode())

ClientMultiSocket.send(str.encode("1"))
resv=ClientMultiSocket.recv(1024)
print(resv.decode())
topic = input('Enter topic: ')

ClientMultiSocket.send(str.encode(topic))
send_file=ClientMultiSocket.recv(1024)
print(send_file.decode())
while True:
    
    File_object=None
    while File_object==None:
        Input2=input("Enter file path: ")
        try:
            File_object = open(Input2)
        except Exception:
            print("File does not exist")
    Final=File_object.read()
    while True:
        try:
            ClientMultiSocket.sendall(str.encode(Final))
        except socket.error as e:
            print(e)
        else:
            break

    res = ClientMultiSocket.recv(1024)
    print(res.decode('utf-8'))
    break
ClientMultiSocket.close()