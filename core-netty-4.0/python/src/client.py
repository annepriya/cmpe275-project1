import comm_pb2
import socket               
import time
import struct


def buildPing(tag, number):

    r = comm_pb2.Request()

    r.body.ping.tag = str(tag)
    r.body.ping.number = number
    
    
    r.header.originator = "python client"
    r.header.tag = str(tag + number + int(round(time.time() * 1000)))
    r.header.routing_id = comm_pb2.Header.PING
    r.header.toNode = str(0)
    
    msg = r.SerializeToString()
    return msg

def buildJob(name_space, jobAction, ownerId):
    
    jobId = str(int(round(time.time() * 1000)))

    r = comm_pb2.Request()    

    r.body.job_op.job_id = jobId
    r.body.job_op.action = jobAction
    
    r.body.job_op.data.name_space = name_space
    r.body.job_op.data.owner_id = ownerId
    r.body.job_op.data.job_id = jobId
    r.body.job_op.data.status = comm_pb2.JobDesc.JOBQUEUED
    
    r.body.job_op.data.options.node_type = comm_pb2.NameValueSet.NODE
    r.body.job_op.data.options.name = "email"
    r.body.job_op.data.options.value = "shajit4"
    
    nvs = comm_pb2.NameValueSet()
    nvs.node_type = comm_pb2.NameValueSet.NODE
    nvs.name = "test"
    nvs.value = "success"
    r.body.job_op.data.options.node.extend([nvs])
    
    r.header.originator = "python client"  
    r.header.routing_id = comm_pb2.Header.JOBS
    r.header.toNode = str(0)
    
    msg = r.SerializeToString()
    return msg

def buildSigninJob(name_space, jobAction, ownerId):
    
    jobId = str(int(round(time.time() * 1000)))

    r = comm_pb2.Request()    

    r.body.job_op.job_id = jobId
    r.body.job_op.action = jobAction
    
    r.body.job_op.data.name_space = name_space
    r.body.job_op.data.owner_id = ownerId
    r.body.job_op.data.job_id = jobId
    r.body.job_op.data.status = comm_pb2.JobDesc.JOBQUEUED
    
    r.body.job_op.data.options.node_type = comm_pb2.NameValueSet.NODE
    r.body.job_op.data.options.name = "operation"
    r.body.job_op.data.options.value = "sign_in"
    
    email = comm_pb2.NameValueSet()
    email.node_type = comm_pb2.NameValueSet.NODE
    email.name = "email"
    email.value = "sinasad@gmail.com"
    
    password = comm_pb2.NameValueSet()
    password.node_type = comm_pb2.NameValueSet.NODE
    password.name = "password"
    password.value = "secret"
    
    fName = comm_pb2.NameValueSet()
    fName.node_type = comm_pb2.NameValueSet.NODE
    fName.name = "firstName"
    fName.value = "Sina"
    
    lName = comm_pb2.NameValueSet()
    lName.node_type = comm_pb2.NameValueSet.NODE
    lName.name = "lastName"
    lName.value = "Nikkhah"
    
    r.body.job_op.data.options.node.extend([email,password,fName,lName])
    
    r.header.originator = "localhost:80"  
    r.header.routing_id = comm_pb2.Header.JOBS
    r.header.toNode = str(0)
    
    msg = r.SerializeToString()
    return msg

def buildNS():
    r = comm_pb2.Request()

    r.body.space_op.action = comm_pb2.NameSpaceOperation.ADDSPACE

    
    r.header.originator = "python client"
    r.header.tag = str(int(round(time.time() * 1000)))
    r.header.routing_id = comm_pb2.Header.NAMESPACES

    m = r.SerializeToString()
    return m

def sendMsg(msg_out, port):
    s = socket.socket()         
    host = socket.gethostname()
   
                  

    s.connect((host, port))        
    msg_len = struct.pack('>L', len(msg_out))    
    s.sendall(msg_len + msg_out)
    len_buf = receiveMsg(s, 4)
    msg_in_len = struct.unpack('>L', len_buf)[0]
    msg_in = receiveMsg(s, msg_in_len)
    
    
    r = comm_pb2.Request()
    r.ParseFromString(msg_in)
    print msg_in
    

    s.close
    
def receiveMsg(socket, n):
    buf = ''
    while n > 0:        
        data = socket.recv(n)                  
        if data == '':
            raise RuntimeError('data not received!')
        buf += data
        n -= len(data)
    return buf  


def getBroadcastMsg(port):
    # listen for the broadcast from the leader"
          
    sock = socket.socket(socket.AF_INET,  # Internet
                        socket.SOCK_DGRAM)  # UDP
   
    sock.bind(('', port))
   
    data = sock.recv(1024)  # buffer size is 1024 bytes
    return data
        
   
if __name__ == '__main__':
    # msg = buildPing(1, 2)
    #UDP_PORT = 8080
    #serverPort = getBroadcastMsg(UDP_PORT)   
    
    name_space = "sign_up"
    ownerId = 123
    signinReq = buildSigninJob(name_space, comm_pb2.JobOperation.ADDJOB, ownerId)    
    sendMsg(signinReq, 5573)   



