import sqlite3
from flask import Flask, render_template, jsonify, request, abort, url_for, Response
from flask_restful import Resource, Api
import json
import requests
from datetime import datetime
import csv

app = Flask(__name__)
api = Api(app)

#Sets the API count in api_count.txt to 0
api_count_f = open('api_count.txt', 'r+')
api_count_f.truncate(0)
api_count_f.seek(0)
api_count_f.write("0")
api_count_f.close()

def is_hex(s):
    try:
        int(s, 16)
        return True
    except ValueError:
        return False

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
        table = 'users'

        #Constructs the data to be parsed by worker
        jdict = {
            "method":"CLEAR",
            "data":{},
            "table":table
        }
        jobject = json.dumps(jdict)
        response = requests.post('http://34.225.55.75:80/api/v1/db/write', json=jobject)
        return Response('{}', status = 200, mimetype='application/json')



# User-related APIs
class Users(Resource):
    def post(self):
        temp = requests.post('http://users/api/v1/_count')
        return Response('{}', status=400, mimetype='application/json')

    # Handles "get users" functionality
    def get(self):
        temp = requests.post('http://users/api/v1/_count')

        jdict = {
            "table": "users",
            "columns": ["username"],
            "where": ""
        }
        jobject = json.dumps(jdict)
        response = requests.post('http://34.225.55.75:80/api/v1/db/read', json=jobject)
        code = response.status_code

        if code != 200:
            final_ret_jobject = '{}'
        else:
            ret = response.json()
            final_ret = []
            for i in ret['rows']:
                final_ret.append(i[0])
            final_ret_jobject = json.dumps(final_ret)
        return Response(final_ret_jobject, status=code, mimetype='application/json')


    #Handles "add user" functionality
    def put(self):
        temp = requests.post('http://users/api/v1/_count')

        try:
            username = request.json['username']
            password = request.json['password']
        except:
            return Response('{}', status=400, mimetype='application/json')
        if not is_hex(password) or len(password) != 40:
            return Response('{}', status=400, mimetype='application/json')

        response = requests.get('http://assignment3-420730437.us-east-1.elb.amazonaws.com:80/api/v1/users')
        code = response.status_code

        #Checks if the username has already been created
        if code == 200:
            ret = response.json()
            if username in ret:
                return Response('{}', status=400, mimetype='application/json')
        elif code != 204:
            return Response('{}', status=400, mimetype='application/json')


        table = 'users'
        jdict = {
            "method": "PUT",
            "data": {
                "username": username,
                "password": password
            },
            "table": table
        }
        jobject = json.dumps(jdict)
        response = requests.post('http://34.225.55.75:80/api/v1/db/write', json=jobject)
        code = 201
        return Response('{}', status=code, mimetype='application/json')


    #Handles "delete user" functionality 
    def delete(self, username):
        temp = requests.post('http://users/api/v1/_count')

        if username == '':
            return Response('{}', status=405, mimetype='application/json')
        else:

            jdict = {
                "table": "users",
                "columns": ["username"],
                "where": " username = '" + username + "'"
            }
            jobject = json.dumps(jdict)
            response = requests.post('http://34.225.55.75:80/api/v1/db/read', json=jobject)
            code = response.status_code

            if code != 200:
                return Response('{}', status=400, mimetype='application/json')


            table = "users"
            jdict = {
                "method": "DELETE",
                "data": {
                    "username": username,
                },
                "table": table
            }
            jobject = json.dumps(jdict)
            response = requests.post('http://34.225.55.75:80/api/v1/db/write', json=jobject)

            rides_jdict = {
                "table": "rides",
                "columns": ["ride_id", "created_by", "users"],
                "where": ""
            }
            rides_jobject = json.dumps(rides_jdict)
            rides_response = requests.post('http://assignment3-420730437.us-east-1.elb.amazonaws.com:80/api/v1/rides/oogabooga', json=rides_jobject)
            rides_code = rides_response.status_code

            code = 200
            if rides_code == 200:
                code = 203
                rides_ret = rides_response.json()
                del_rides = []
                for i in range(len(rides_ret['rows'])):
                    if rides_ret['rows'][i][1] == username:
                        ride_id = rides_ret['rows'][i][0]
                        temp_response = requests.delete('http://assignment3-420730437.us-east-1.elb.amazonaws.com:80/api/v1/rides/'+ride_id)
                        del_rides.append(i)

                    else:
                        temp = rides_ret['rows'][i][2].split(',')
                        try:
                            temp.remove(username)
                        except:
                            pass
                        rides_ret['rows'][i][2] = ','.join(temp)

                for i in del_rides:
                    rides_ret['rows'].pop(i)

                for rec in rides_ret['rows']:
                    ride_id = str(rec[0])
                    users = rec[2]
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
                    response = requests.put('http://assignment3-420730437.us-east-1.elb.amazonaws.com:80/api/v1/rides/oogabooga', json=jobject)
                    
            return Response('{}', status=code, mimetype='application/json')




api.add_resource(API_Access_Monitor, '/api/v1/_count')
api.add_resource(Users,
                 '/api/v1/users',
                 '/api/v1/users/<string:username>')
api.add_resource(DatabaseClear, '/api/v1/db/clear')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='80', debug=True)
