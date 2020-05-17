import sqlite3
from flask import Flask, render_template, jsonify, request, abort, url_for, Response
from flask_restful import Resource, Api
import json
import requests
from datetime import datetime
import csv

app = Flask(__name__)
api = Api(app)

#Sets the API count to 0
api_count_f = open('api_count.txt', 'r+')
api_count_f.truncate(0)
api_count_f.seek(0)
api_count_f.write("0")
api_count_f.close()

def convertTime(timestamp):
    return datetime.strptime(timestamp, "%d-%m-%Y:%S-%M-%H")

#Checks area codes
def check_area_num(num):
    s_num = str(num)
    csvfile = open('AreaNameEnum.csv')
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        if s_num == row[0]:
            csvfile.close()
            return True
    csvfile.close()
    return False


#For API Counts
class API_Access_Monitor(Resource):
    def post(self):
        try:
            f = open('api_count.txt', 'r+')
            s = f.readline()
            s = int(s)
            s += 1
            f.truncate(0)
            f.seek(0)
            f.write(str(s))
            f.close()
            code = 200
        except:
            code = 405
        return Response('{}', status=code, mimetype='application/json')

    def get(self):
        try:
            f = open('api_count.txt', 'r+')
            s = f.readline()
            s = int(s)
            api_count = [s]
            f.close()
            ret = json.dumps(api_count)
            code = 200
        except:
            ret = '{}'
            code = 405
        return Response(ret, status=code, mimetype='application/json')

    def delete(self):
        try:
            f = open('api_count.txt', 'r+')
            s = 0
            f.truncate(0)
            f.seek(0)
            f.write(str(s))
            f.close()
            code = 200
        except:
            code = 405
        return Response('{}', status=code, mimetype='application/json')





# API to handle the "clear database" functionality
class DatabaseClear(Resource):
    def post(self):
        table = 'rides'

        #Constructs the data to be parsed by worker
        jdict = {
            "method":"CLEAR",
            "data":{},
            "table":table
        }
        jobject = json.dumps(jdict)

        response = requests.post('http://34.225.55.75:80/api/v1/db/write', json=jobject)
        return Response('{}', status = 200, mimetype='application/json')



class Oogabooga(Resource):
    def post(self):
        jdict = {
            "table": request.json['table'],
            "columns": request.json['columns'],
            "where": request.json['where']
        }
        jobject = json.dumps(jdict)
        response = requests.post('http://34.225.55.75:80/api/v1/db/read', json=jobject)
        code = response.status_code
        ret = '{}'
        if code == 200:
            ret = response.json()

        return Response(ret, status=code, mimetype='application/json')

    def put(self):
        jdict = {
            "method": request.json['method'],
            "data": request.json['data'],
            "table": request.json['table']
        }
        jobject = json.dumps(jdict)
        response = requests.post('http://34.225.55.75:80/api/v1/db/write', json=jobject)
        code = response.status_code

        return Response('{}', status=code, mimetype='application/json')


#Ride Counter
class Ride_Counter__________for_now(Resource):
    def post(self):
        temp = requests.post('http://rides/api/v1/_count')
        return Response('{}', status=400, mimetype='application/json')

    def put(self):
        temp = requests.post('http://rides/api/v1/_count')
        return Response('{}', status=400, mimetype='application/json')

    def delete(self):
        temp = requests.post('http://rides/api/v1/_count')
        return Response('{}', status=400, mimetype='application/json')

    def get(self):
        temp = requests.post('http://rides/api/v1/_count')
        jdict = {
            "table": "rides",
            "columns": [],
            "where": ""
        }
        jobject = json.dumps(jdict)
        response = requests.post('http://34.225.55.75:80/api/v1/db/read', json=jobject)
        code=  response.status_code
        if code != 200:
            return Response('{}', status = 405, mimetype= 'application/json')

        ret = response.json()
        ret_ride_count = ret['rows'][0]
        final_ret_jobject = json.dumps(ret_ride_count)
        return Response(final_ret_jobject, status=code, mimetype='application/json')


#Ride-related APIs
class Rides(Resource):
    def put(self):
        temp = requests.post('http://rides/api/v1/_count')
        return Response('{}', status=400, mimetype='application/json')

    #Handles "delete ride" functionality
    def delete(self, ride_id):
        temp = requests.post('http://rides/api/v1/_count')
        if ride_id == '':
            code = 400
        else:

            jdict = {
                "table": "rides",
                "columns": ["ride_id"],
                "where": " ride_id = " + str(ride_id)
            }
            jobject = json.dumps(jdict)
            response = requests.post('http://34.225.55.75:80/api/v1/db/read', json=jobject)
            code = response.status_code

            if code == 200:
                table = "rides"
                jdict = {
                    "method": "DELETE",
                    "data": {
                        "ride_id": ride_id,
                    },
                    "table": table
                }
                jobject = json.dumps(jdict)
                response = requests.post('http://34.225.55.75:80/api/v1/db/write', json=jobject)
                code = 200
            else:
                code = 400

        return Response('{}', status=code, mimetype='application/json')



    def get(self, ride_id=''):
        temp = requests.post('http://rides/api/v1/_count')

        if(ride_id):
            jdict = {
                "table": "rides",
                "columns": ["ride_id", "created_by", "users", "timestamp", "source", "destination"],
                "where": " ride_id = " + str(ride_id)
            }
            jobject = json.dumps(jdict)
            response = requests.post('http://34.225.55.75:80/api/v1/db/read', json=jobject)
            code = response.status_code

            if code != 200:
                final_ret_jobject = '{}'
            else:
                ret = response.json()
                ret_ride_id = ret['rows'][0][0]
                ret_created_by = ret['rows'][0][1]
                ret_users = ret['rows'][0][2]
                ret_timestamp = ret['rows'][0][3]
                ret_source = ret['rows'][0][4]
                ret_destination = ret['rows'][0][5]
                final_ret = {
                    "ride_id": ret_ride_id,
                    "Created_by": ret_created_by,
                    "users": ret_users,
                    "timestamp": ret_timestamp,
                    "source": ret_source,
                    "destination": ret_destination
                }
                final_ret_jobject = json.dumps(final_ret)

        else:
            try:
                source = request.args.get('source')
                destination = request.args.get('destination')
            except:
                return Response('', status=400, mimetype='application/json')

            try:
                source = int(source)
                destination = int(destination)
                jdict = {
                    "table": "rides",
                    "columns": ["ride_id", "created_by", "timestamp"],
                    "where": " source = " + str(source) + " AND destination = " + str(destination)
                }
                jobject = json.dumps(jdict)
                response = requests.post('http://34.225.55.75:80/api/v1/db/read', json=jobject)
                code = response.status_code

                if code == 200:
                    ret = response.json()
                    ret_list = ret['rows']
                    time_now = datetime.now()
                    final_ret = []
                    for i in ret_list:
                        ride_id = i[0]
                        username = i[1]
                        timestamp = i[2]
                        temp = convertTime(timestamp)
                        if temp > time_now:
                            d = {
                                "ride_id": ride_id,
                                "username": username,
                                "timestamp": timestamp
                            }
                            final_ret.append(d)
                    code = 200
                else:
                    final_ret = ''
                    code == 204
            

            except:
                code = 400
                final_ret = ''
            final_ret_jobject = json.dumps(final_ret)

        return Response(final_ret_jobject, status=code, mimetype='application/json')


    def post(self, ride_id=''):
        temp = requests.post('http://rides/api/v1/_count')

        if ride_id != '':
            try:
                username = request.json['username']
                print(username)
            except:
                return Response('{}', status=400, mimetype='application/json')

            if username == '':
                return Response('{}', status=400, mimetype='application/json')
            response = requests.get('http://assignment3-420730437.us-east-1.elb.amazonaws.com:80/api/v1/users')
            code = response.status_code

            if code !=200:
                return Response('{}', status=400, mimetype='application/json')
            else:
                ret = response.json()
                if username not in ret:
                    return Response('{}', status=400, mimetype='application/json')

            jdict1 = {
                "table": "rides",
                "columns": ["ride_id", "created_by", "users", "timestamp"],
                "where": " ride_id = " + str(ride_id)
            }
            jobject = json.dumps(jdict1)
            response = requests.post('http://34.225.55.75:80/api/v1/db/read', json=jobject)
            code = response.status_code

            if code != 200:
                return Response('{}', status=400, mimetype='application/json')

            ret = response.json()
            ride_id = ret['rows'][0][0]
            created_by = ret['rows'][0][1]
            users = ret['rows'][0][2]
            users = users.split(',')
            timestamp = ret['rows'][0][3]

            if username == created_by:
                return Response('{}', status=400, mimetype='application/json')
            if convertTime(timestamp) < datetime.now():
                return Response('{}', status=400, mimetype='application/json')
            if username in users:
                return Response('{}', status=400, mimetype='application/json')
            users.append(username)

            try:
                users.remove('')
            except:
                pass



            users = ','.join(users)
            table = "rides"
            jdict2 = {
                "method": "POST",
                "data": {
                    "ride_id": ride_id,
                    "users": users,
                },
                "table": table
            }
            jobject = json.dumps(jdict2)
            response = requests.post('http://34.225.55.75:80/api/v1/db/write', json=jobject)
            return Response('{}', status=code, mimetype='application/json')


        else:
            try:
                created_by = request.json['created_by']
                timestamp = request.json['timestamp']
                source = request.json['source']
                destination = request.json['destination']
            except:
                return Response('{}', status=400, mimetype='application/json')

            response = requests.get('http://assignment3-420730437.us-east-1.elb.amazonaws.com:80/api/v1/users')
            code = response.status_code
            if code != 200:
                return Response('{}', status=400, mimetype='application/json')
            else:
                ret = response.json()
                if created_by not in ret:
                    return Response('{}', status=400, mimetype='application/json')

            if created_by == '' or timestamp == '' or check_area_num(source) == False or check_area_num(destination) == False:
                return Response('{}', status=400, mimetype='application/json')
            if convertTime(timestamp) < datetime.now():
                return Response('{}', status=400, mimetype='application/json')
            table = "rides"
            jdict = {
                "method": "POST",
                "data": {
                    "created_by": created_by,
                    "timestamp": timestamp,
                    "source": source,
                    "destination": destination,
                    "users": ""
                },
                "table": table
            }
            jobject = json.dumps(jdict)
            response = requests.post('http://34.225.55.75:80/api/v1/db/write', json=jobject)
            code = 201
            return Response('{}', status=code, mimetype='application/json')



api.add_resource(API_Access_Monitor, '/api/v1/_count')
api.add_resource(Oogabooga, '/api/v1/rides/oogabooga')
api.add_resource(Ride_Counter__________for_now, '/api/v1/rides/count')
api.add_resource(Rides, '/api/v1/rides',
                        '/api/v1/rides/<int:ride_id>')
api.add_resource(DatabaseClear, '/api/v1/db/clear')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='80', debug=True)
