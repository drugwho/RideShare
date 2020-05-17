from flask import Flask,Response
from flask import request
import json
import uuid
import logging
import threading
import time

import docker
import pika
from kazoo.client import KazooClient
from kazoo.client import KazooState

corr_id = str(uuid.uuid4())
app = Flask(__name__)
logging.basicConfig()





#INITIALIZING CLIENTS
zk = KazooClient(hosts='zookeeper:2181',timeout=1.0)
zk.start(timeout=1)

apiclient = docker.APIClient(base_url = "unix://var/run/docker.sock")
client = docker.from_env()




#DICTIONARIES FOR CONTAINERS AND THEIR PIDs
ids = dict()
master_id = dict()

#Function to add to the dictionaries
def add_ids(container):
    try:
        c_type = container['Labels']['worker_type']
        if(c_type == 'slave'):
            ids[container['Id']] = apiclient.inspect_container(container['Id'])['State']['Pid']
        else:
            master_id[container['Id']]=apiclient.inspect_container(container['Id'])['State']['Pid']
    except:
        pass

#Function to delete from the dictionaries
def delete_ids(id):
   for key in ids:
       if key == id:
           ids.pop(key)
           break

#Adding existing containers to the dictionaries
apiconts = apiclient.containers()
for apicont in apiconts:
     add_ids(apicont)




#VARIABLE TO RESPAWN CONTAINERS
container_respawn = True

#LOCKING VARIABLES
r_lock = 0
a_lock = 0
req_lock = 0
no_of_requests = 0

#RETURN VARIABLES FOR DB READS AND WRITES
ret =  dict()
status = 0
write_stat = 0
write_ret=dict()




#SETTING UP INITIAL ZOOKEEPER STRUCTURE
mpath = '/master'
spath = '/slaves'
sspath = '/slaves/slave'

zk.delete(mpath, recursive=True)
zk.delete(spath, recursive=True)

if zk.exists(mpath):
    print("We have a master.")
else:
   zk.create(mpath,b'')

if zk.exists(spath):
    print("We have slaves.")
else:
    zk.create(spath, b'')

mdata = ''
for key in master_id:
    mdata = key+'pid'+str(master_id[key])

sdata='All the slaves.'.encode()

zk.set(mpath,mdata.encode())
zk.set(spath,sdata)




#FUNCTION TO CREATE A ZOOKEEPER NODE
def create_node():
    for key in ids:
        sp = sspath+key
        if zk.exists(sp):
            pass
        else:
            spd = key+'pid'+str(ids[key])
            zk.create(sp, spd.encode())

#Creating a node for the first slave
create_node()





#FUNCTION TO ADD A SLAVE
def add_container():
    new_cont = client.containers.create(image='proj_mk_2_slave',\
                                    volumes={'/var/run/docker.sock':{'bind':'/var/run/docker.sock', 'mode':'rw'},\
                                             '/home/ubuntu/proj_mk_2':{'bind':'/code','mode':'rw'}},\
                                    command='python3 /code/worker.py',\
                                    environment=['WORKER_TYPE=SLAVE'],\
                                    labels={"worker_type":"slave"},\
                                    network="proj_mk_2_default",\
                                    detach = True)
    new_cont.start()
    id = new_cont.id
    apiconts = apiclient.containers()
    for apicont in apiconts:
         if apicont['Id']==id:
             add_ids(apicont)
    create_node()
    print("---IN ADD_CONTAINER; NO. OF SLAVES AFTER ADDING: ",len(apiconts)-4,"---")

#FUNCTION TO REMOVE A SLAVE
def remove_container():
    max_pid = -1
    for key in ids:
        if ids[key]>=max_pid:
            max_pid = ids[key]
            max_cid = key
    znode = sspath+max_cid
    zk.delete(znode, recursive = True)
    container = client.containers.get(max_cid)
    try:
        container.kill()
        code = 200
        ret_val = max_pid
        print("---SLAVE KILLED---")
    except:
        print("---UNABLE TO KILL SLAVE---")
        code = 400
        ret_val = 0
    if code==200:
        delete_ids(max_cid)
        cl = docker.from_env()
        l = cl.containers.list()
        print("NO. OF SLAVES AFTER DELETE: ",len(l)-4)
    return ret_val



#WATCHING

#Function To Watch Slaves
def watch_slaves(event):
    global container_respawn
    global a_lock
    if event.type=='CHILD' and container_respawn==True:
        while(a_lock == 1):
            continue
        a_lock = 1
        print("---ADDING IN WATCH_SLAVES FUNCTION---")
        add_container()
        a_lock = 0
    else:
        print("---IN WATCH_SLAVES FUNCTION; NOT ADDING---")
    zk.get_children(spath,watch=watch_slaves)

#Function To Watch The Master
def watch_master(event):
    print("---WATCHING MASTER---")
    zk.get(mpath,watch=watch_master)

#Setting Up Initial Watches
slaves = zk.get_children(spath,watch=watch_slaves)
mc = zk.get(mpath,watch=watch_master)




#Function to manually add more slaves
trial = 0
while(trial):
    add_container()
    trial-=1

#FUNCTION TO PERFORM SCALING
def scale():
    print("---SCALING THREAD STARTED---")
    global container_respawn
    global no_of_requests
    global r_lock
    global a_lock
    global req_lock
    sclient = docker.from_env()
    interval = 120
    change_by = 20
    while(True):
        time.sleep(interval)
        scontainers = sclient.containers.list()
        no_of_slaves = len(scontainers)-4
        temp = no_of_requests%change_by
        if temp == 0 and no_of_requests != 0:
            required = no_of_requests//change_by
        else:
            required = no_of_requests//change_by + 1
        print()
        print("No. of Requests :", no_of_requests)
        print("Required Slaves :", required)
        print("No. of Slaves Currently Available :", no_of_slaves)
        print()
        if no_of_slaves>required:
            no = no_of_slaves-required
            while(r_lock==1):
                continue
            r_lock = 1
            for i in range(0,no):
                container_respawn = False
                code = remove_container()
            r_lock = 0
        elif no_of_slaves<required:
            no = required-no_of_slaves
            while(a_lock==1):
                continue
            a_lock = 1
            for i in range(0,no):
                add_container()
            a_lock = 0
        else:
            pass
        while(req_lock == 1):
            continue
        req_lock = 1
        no_of_requests = 0
        req_lock = 0




#FUNCTIONS THAT RECEIVE FROM RESPONSE QUEUES

#ResponseQ (For Reads)
def on_res(ch, method, props, body):
    if corr_id == props.correlation_id:
        qwerty=body
        body = eval(qwerty.decode('utf-8'))
        global status
        status = body['status']
        global ret
        ret = body['ret']
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

#WriteresQ (For Writes)
def write_res(ch, method, props, body):
    if corr_id == props.correlation_id:
        qwerty=body
        body = eval(qwerty.decode('utf-8'))
        global write_stat
        write_stat = body['status']
        global write_ret
        write_ret = body['ret']
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()




#FUNCTIONS THAT HANDLE ROUTING OF DB READS AND WRITES

#Read Requests
def rab(sev,mes):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
    channel.queue_declare(queue='responseQ')
    severity = sev
    message=mes
    channel.queue_bind(exchange='direct_logs',queue='responseQ', routing_key='response')
    channel.basic_publish(exchange='direct_logs', routing_key=severity, properties=pika.BasicProperties(reply_to='responseQ', correlation_id=corr_id), body=message)
    print("SENT %r:%r" % (severity, message))
    channel.basic_qos(prefetch_count=1)
    ct = channel.basic_consume(queue='responseQ', on_message_callback=on_res)
    channel.start_consuming()
    print("---READ RESPONSE RECEIVED---")
    connection.close()

#Write Requests
def write_rab(sev,mes):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
    channel.queue_declare(queue='writeresQ',exclusive=True)
    severity = sev
    message=mes
    channel.queue_bind(exchange='direct_logs',queue='writeresQ', routing_key='writeres')
    channel.basic_publish(exchange='direct_logs', routing_key=severity, properties=pika.BasicProperties(reply_to='writeresQ', correlation_id=corr_id), body=message)
    print("SENT %r:%r" % (severity, message))
    channel.basic_qos(prefetch_count=1)
    ct = channel.basic_consume(queue='writeresQ', on_message_callback=write_res)
    channel.start_consuming()
    print("---WRITE RESPONSE RECEIVED---")
    connection.close()










#API CALLS AND ASSOCIATED FUNCTIONS
@app.route('/api/v1/db/read',methods=['POST'])
def read():
      global no_of_requests
      global req_lock
      while (req_lock==1):
          continue
      req_lock = 1
      no_of_requests +=1
      req_lock = 0
      req = request.json
      rab('read',str(req))
      #print("---LOGS---")
      cl=docker.from_env()
      ct = cl.containers.list()
      for c in ct:
          try:
              ctype = c.labels['worker_type']
              if ctype == 'slave' or ctype == 'master':
                  lcont = cl.containers.get(c.id)
                  #print(lcont.name, "------------| ")
                  #print(lcont.logs())
          except:
              pass
      #print('---END OF LOGS---')
      global ret
      global status
      ret = json.dumps(ret)
      return(Response(ret,status=status,mimetype='application/json'))

@app.route('/api/v1/db/write',methods=['POST'])
def write():
    req = request.json
    write_rab('write',str(req))
    #print("---LOGS---")
    cl=docker.from_env()
    ct = cl.containers.list()
    for c in ct:
          try:
              ctype = c.labels['worker_type']
              if ctype == 'slave' or ctype == 'master':
                  lcont = cl.containers.get(c.id)
                  #print(lcont.name, "------------| ")
                  #print(lcont.logs())
          except:
              pass
    #print('---END OF LOGS---')
    global write_ret
    global write_stat
    write_ret = json.dumps(write_ret)
    return(Response(write_ret,status=write_stat,mimetype='application/json'))

@app.route('/api/v1/crash/master',methods=['POST'])
def crash_master():
    return(Response(json.dumps({}),status=405,mimetype='application/json'))

@app.route('/api/v1/crash/slave', methods=['POST'])
def crash_slave():
    global r_lock
    global container_respawn
    while(r_lock == 1):
        continue
    r_lock = 1
    container_respawn = True
    ret_val = remove_container()
    r_lock = 0
    di = dict()
    di[0]=ret_val
    return(Response(json.dumps(di[0]),status=200,mimetype='application/json'))

@app.route('/api/v1/worker/list',methods = ['GET'])
def worker_list():
    wclient = docker.APIClient()
    wcontainers = wclient.containers()
    l_pids=[]
    for wcontainer in wcontainers:
        try:
            ctype = wcontainer['Labels']['worker_type']
            if ctype == 'master' or ctype == 'slave':
                l_pids.append(wclient.inspect_container(wcontainer['Id'])['State']['Pid'])
        except:
            pass
    l_pids.sort()
    ret_val = dict()
    ret_val[0]=l_pids
    return(Response(json.dumps(ret_val[0]),status=200,mimetype='application/json'))

if __name__== "__main__":
    scaling_thread = threading.Thread(target=scale, daemon = True)
    scaling_thread.start()
    app.run(host="0.0.0.0", port=80)