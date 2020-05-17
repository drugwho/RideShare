import sqlite3
import json
from datetime import datetime
import pika
import time
import os
import subprocess

#SETTING UP CONNECTIONS AND EXCHANGES
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
#Exchange for readQ, writeQ, responseQ, writeresQ (temporary)
channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
#Exchange for syncQ
channel.exchange_declare(exchange='sync', exchange_type='fanout')

#DECLARING QUEUES BASED ON WORKER_TYPE
e_type = os.environ['WORKER_TYPE']
if e_type=='MASTER':
        channel.queue_declare(queue='writeQ')
        channel.queue_bind(exchange='direct_logs', queue='writeQ', routing_key='write')
elif e_type=='SLAVE':
        channel.queue_declare(queue='readQ')
        r = channel.queue_declare(queue='',exclusive=True) #Queue for syncing
        qname = r.method.queue
        channel.queue_bind(exchange='direct_logs',queue='readQ', routing_key='read')
        channel.queue_bind(exchange='sync',queue=qname)
print('---WAITING FOR LOGS. TO EXIT, PRESS CTRL+C---')










#FUNCTION THAT HANDLES MESSAGES IN THE READQ
def read_req(ch, method, properties, body):
    #Converts body to a usable dictionary
    c = body
    body = c.decode('utf-8','strict')
    jobj = body
    jobj = json.dumps(jobj)
    temp = eval(json.loads(jobj))
    table = columns = where = ''
    
    #Extracts information from body
    try:
        table = temp['table']
        columns = temp['columns']
        where = temp['where']
    except:
        d = {'ret': {}, 'status': 400}
        j = json.dumps(d)
        final_pub_val = j

    #Generates SQL command 
    command = ''
    if len(columns):
        command = 'SELECT ' + ', '.join(columns) + ' FROM ' + table
    else:
        command = 'SELECT COUNT(*) from ' + table
    if len(where):
        command = command + ' WHERE ' + where

    
    try:
        with sqlite3.connect('database.db') as con:
            con.execute('PRAGMA foreign_keys = 1')
            cur = con.cursor()
            cur.execute(command)
            rows = cur.fetchall()
            #If the required fields are empty, set return status as 204
            if not len(rows): 
                code = 204
                ret = ''
                d = {'ret':{},'status':204}
                d = json.dumps(d)
            else:
                ret = {'rows': rows}
                d = {'ret': ret, 'status': 200}
                d = json.dumps(d)
                ret = json.dumps(ret)
            con.commit()
    
    #If DB read failed, set return status as 400
    except:
        try:
            con.rollback()
        except:
            pass
        d = {'ret': {}, 'status': 400}
        d = json.dumps(d)

    finally:
        try:
            con.close()
        except:
            pass
        final_pub_val = d


    #Publish response to ResponseQ
    ch.basic_publish(exchange='direct_logs', routing_key = 'response', properties=pika.BasicProperties(correlation_id=properties.correlation_id), body=final_pub_val)
    ch.basic_ack(delivery_tag=method.delivery_tag)










#FUNCTION THAT HANDLES MESSAGES IN THE WRITEQ
def write_req(ch, methods, properties, body):
    #final is the object used for syncing databases
    final = body
    #Converts body into a usable dictionary
    c = body
    body = c.decode('utf-8','strict')
    jobj = json.dumps(body)
    temp = eval(json.loads(jobj))
    table=method=''
    data = dict()
    
    #Extracts the information to write
    try:
        data = temp['data']
        table = temp['table']
        method = temp['method']
    except:
        pass





    if table == 'users':
        if method == 'CLEAR':
            with sqlite3.connect('database.db') as con:
                try:
                    con.execute('PRAGMA foreign_keys = 1')
                    cur = con.cursor()
                    cur.execute("DELETE FROM users")
                    con.commit()
                    code = 200
                    d = {'ret': {}, 'status': code}
                    d = json.dumps(d)
                except:
                    con.rollback()
                    code = 400
                    d = {'ret': {}, 'status': code}
                    d = json.dumps(d)
            final_pub_val = d



        elif method == 'PUT':
            username = data['username']
            password = data['password']

            try:
                with sqlite3.connect('database.db') as con:
                    con.execute('PRAGMA foreign_keys = 1')
                    cur = con.cursor()
                    cur.execute('INSERT INTO ' + table
                                + " (username, password) \
                            VALUES (?,?)"
                                , (username, password))
                    con.commit()
                    code = 201
                    d = {'ret': {}, 'status': 201}
                    d = json.dumps(d)

            except:
                try:
                    con.rollback()
                except:
                    pass
                d = {'ret': {}, 'status': 400}
                d = json.dumps(d)

            finally:
                try:
                    con.close()
                except:
                    pass

            final_pub_val = d



        elif method == 'DELETE':
            username = data['username']

            try:
                with sqlite3.connect('database.db') as con:
                    con.execute('PRAGMA foreign_keys = 1')
                    cur = con.cursor()
                    command = 'DELETE FROM ' + table \
                        + " WHERE username = '" + username + "'"
                    cur.execute(command)
                    con.commit()
                    d = {'ret': {}, 'status': 200}
                    d = json.dumps(d)

            except:
                try:
                    con.rollback()
                except:
                    pass
                d = {'ret': {}, 'status': 400}
                d = json.dumps(d)

            finally:
                try:
                    con.close()
                except:
                    pass

            final_pub_val = d





    elif table == 'rides':
        if method == 'CLEAR':
            with sqlite3.connect('database.db') as con:

                try:
                    con.execute('PRAGMA foreign_keys = 1')
                    cur = con.cursor()
                    cur.execute("DELETE FROM rides")
                    con.commit()
                    code = 200
                    d = {'ret': {}, 'status': 200}
                    d = json.dumps(d)

                except:
                    con.rollback()
                    code = 400
                    d = {'ret': {}, 'status': 400}
                    d = json.dumps(d)

            final_pub_val = d

        elif method == 'POST':
            #If ride_id already exists, updates the users in the ride
            if 'ride_id' in data.keys():
                ride_id = data['ride_id']
                users = data['users']
                command = "UPDATE rides SET users ='" + users \
                    + "' WHERE ride_id = " + str(ride_id)

                with sqlite3.connect('database.db') as con:
                    try:
                        con.execute('PRAGMA foreign_keys = 1')
                        cur = con.cursor()
                        cur.execute(command)
                        con.commit()
                        code = 200
                        d = {'ret': {}, 'status': 200}
                        d = json.dumps(d)

                    except:
                        con.rollback()
                        code = 400
                        d = {'ret': {}, 'status': 400}
                        d = json.dumps(d)
            
            #Otherwise, creates new ride
            else:
                created_by = data['created_by']
                timestamp = data['timestamp']
                source = data['source']
                destination = data['destination']
                users = data['users']
                f = open('ride_id.txt', 'r+')
                s = f.readline()
                s = int(s)
                ride_id = s
                #If slave, does not create a new ride_id; uses the one the master used, for consistency
                if e_type=='SLAVE':
                        ride_id -= 1
                        s -= 1
                s += 1
                f.truncate(0)
                f.seek(0)
                f.write(str(s))
                f.close()

                try:
                    with sqlite3.connect('database.db') as con:
                        con.execute('PRAGMA foreign_keys = 1')
                        cur = con.cursor()
                        cur.execute('INSERT INTO ' + table
                                    + " (ride_id, created_by, timestamp, source, destination, users) \
                                VALUES (?,?,?,?,?,?)"
                                    , (
                            ride_id,
                            created_by,
                            timestamp,
                            source,
                            destination,
                            users,
                            ))
                        con.commit()
                        code = 201
                        d = {'ret': {}, 'status': 201}
                        d = json.dumps(d)

                except:
                    try:
                        con.rollback()
                    except:
                        pass
                    code = 400
                    d = {'ret': {}, 'status': 400}
                    d = json.dumps(d)

                finally:
                    try:
                        con.close()
                    except:
                        pass

            final_pub_val = d



        elif method == 'DELETE':
            ride_id = data['ride_id']

            try:
                with sqlite3.connect('database.db') as con:
                    con.execute('PRAGMA foreign_keys = 1')
                    cur = con.cursor()
                    command = 'DELETE FROM ' + table \
                        + ' WHERE ride_id = ' + str(ride_id)
                    cur.execute(command)
                    temp = cur.fetchall()
                    con.commit()
                    d = {'ret': {}, 'status': 200}
                    d = json.dumps(d)

            except:
                try:
                    con.rollback()
                except:
                    pass
                d = {'ret': {}, 'status': 400}
                d = json.dumps(d)

            finally:
                try:
                    con.close()
                except:
                    pass

            final_pub_val = d


    #Publishing responses to temporary WriteresQ
    if e_type == 'MASTER':
        ch.basic_publish(exchange='sync', routing_key='', properties=pika.BasicProperties(correlation_id=properties.correlation_id), body=final)
        ch.basic_publish(exchange='direct_logs', routing_key='writeres', properties=pika.BasicProperties(correlation_id=properties.correlation_id), body=final_pub_val)
    ch.basic_ack(delivery_tag=methods.delivery_tag)









#CONSUMING FROM QUEUES
if e_type=='SLAVE':
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='readQ', on_message_callback=read_req)
    #SyncQ
    channel.basic_consume(queue=qname, on_message_callback=write_req)
else:
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='writeQ', on_message_callback=write_req)
channel.start_consuming() 