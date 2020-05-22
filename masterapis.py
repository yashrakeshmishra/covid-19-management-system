from flask import Flask, request, make_response, jsonify, abort
from flask_restful import Resource, Api
from flask_restful import reqparse
import pika
import pyorient
import sys
import json
import DBLauncher
import Covidfunctions
import RTR
from multiprocessing import Process, Queue
import time


checkFlag = 0
maxSize = 0
q = Queue(maxSize)
q1 = Queue(maxSize)

app = Flask(__name__)
api = Api(app)

DBLauncher.reset_db()
DBLauncher.load_db()
DBLauncher.read_hospitaldata()

last_time = time.time()

#--------------------------------------------------#
#                  process t3                      #
#--------------------------------------------------#
def timequery():
    global last_time
    curr_time = time.time()
    if curr_time - last_time >= 15:
        print("Executing TimeQuery")
        # database name
        dbname = "covid19-ky"
        # database login is root by default
        login = "root"
        # database password, set by docker param
        password = "rootpwd"

        # create client to connect to local orientdb docker container
        client = pyorient.OrientDB("localhost", 2424)
        session_id = client.connect(login, password)

        if client.db_exists(dbname):
            #open the database
            client.db_open(dbname,login,password)
            zips = client.query("SELECT zipcode FROM zipcodes WHERE positive_count >= 2 * last_count AND last_count !=0")
            ziplists = str(len(zips))
            if int(ziplists) >=5:
                client.command("UPDATE zipcodes SET statealert = 1")
            client.command("UPDATE zipcodes SET last_count = positive_count")
            client.close()
        print("Closing TimeQuery")
        last_time = time.time()

#--------------------------------------------------#
#           storing data from RabbitMQ             #
#--------------------------------------------------#
def data_fetch():
    time.sleep(10)
    username = 'student'
    pword = 'student01'
    hostname = '128.163.202.61'
    virtualhost = 'patient_feed'

    credentials = pika.PlainCredentials(username, pword)
    parameters = pika.ConnectionParameters(hostname,
                                           5672,
                                           virtualhost,
                                           credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    exchange_name = 'patient_data'
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic')
    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    binding_keys = "#"

    if not binding_keys:
        sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
        sys.exit(1)

    for binding_key in binding_keys:
        channel.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=binding_key)



    def callback(ch, method, properties, body):
        queueFlag = 0
        bodystr = body.decode('utf-8-sig')
        data = json.loads(bodystr)
        if queueFlag ==0 or queueFlag == 1:
            q.put(data)
            queueFlag = 2
        if queueFlag == 2:
            q1.put(data)
            queueFlag = 1

    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


#--------------------------------------------------#
#       Process and send data to database          #
#--------------------------------------------------#
def sendData():
    senddataFlag = 0
    time.sleep(15)
    while(True):
        if senddataFlag == 0 or senddataFlag == 1:
            if(q.empty() == True):
                pass
            else:
                data = q.get(0)
                for payload in data:
                    mrn = payload['mrn']
                    zipcode = payload['zip_code']
                    patient_status_code = payload['patient_status_code']
                    Covidfunctions.readpatients(mrn,zipcode,patient_status_code)
                    senddataFlag = 2
                    continue
        if senddataFlag == 2:
            if(q1.empty() == True):
                pass
            else:
                data = q1.get(0)
                for payload in data:
                    mrn = payload['mrn']
                    zipcode = payload['zip_code']
                    patient_status_code = payload['patient_status_code']
                    Covidfunctions.readpatients(mrn,zipcode,patient_status_code)
                    senddataFlag = 1
                    continue
        timequery()

#--------------------------------------------------#
#                  MF1 API                         #
#--------------------------------------------------#
# MF 1: API to display team details and app status
@app.route('/api/getteam', methods = ['GET'])
def getteam():
    team_name = 'Team AXY'
    team_member_ids = '12405067','12314681','12463865'
    app_status_code = 0
    result = {'team_name': team_name, 'team_member_ids': team_member_ids, 'app_status_code': app_status_code}
    return result

#--------------------------------------------------#
#                  MF2 API                         #
#--------------------------------------------------#
# MF 2: API to reset the database and all markers
@app.route('/api/reset', methods = ['GET'])
def reset():
    reset_status_code = '0'
    reset_status_code = DBLauncher.resetRecords()
    global sending
    global rabbit
    global checkFlag
    if checkFlag == 0:
        rabbit.start()
        sending.start()
        checkFlag = 1
    result = {'reset_status_code':reset_status_code}
    return result


#--------------------------------------------------#
#                  OF2 API                         #
#--------------------------------------------------#
# OF 2: API search by mrn patients location (home or specific hospital)
@app.route('/api/getpatient/<string:mrn>', methods = ['GET'])
def getpatient(mrn):
   patient_location = Covidfunctions.OF2(mrn)
   return {"mrn": mrn, "location_code": patient_location}


#--------------------------------------------------#
#                  OF3 API                         #
#--------------------------------------------------#
# OF 3: API to report hospital patient numbers
@app.route('/api/gethospital/<string:id>', methods = ['GET'])
def gethospital(id):
   hospital = Covidfunctions.OF3(id)
   return {"id": id, "name": hospital[0], "zipcode": hospital[3], "total_beds": hospital[1],
           "available_beds": hospital[2]}



#--------------------------------------------------#
#                 RTR1 API                         #
#--------------------------------------------------#
# RTR 1: API to alert on zipcode if count(patient_stat_code == 2 or 5 or 6) >= 2* count(prev)
@app.route('/api/zipalertlist', methods = ['GET'])
def zipalert():
    ziplist = RTR.zipcounter()
    if len(ziplist) == 0:
       msg = "No zipcodes are in alert state at the moment!"
       return {"zipalert":msg}
    return jsonify(ziplist)


#--------------------------------------------------#
#                 RTR2 API                         #
#--------------------------------------------------#
# RTR 2: API to alert on statewide if zipcode alert >=5 within same 15s window
@app.route('/api/alertlist', methods = ['GET'])
def statealert():
    state_stat = RTR.statewide()
    return {"state_status": state_stat}

#--------------------------------------------------#
#                 RTR3 API                         #
#--------------------------------------------------#
# RTR 3: API to return statewide positive and negative testcount
@app.route('/api/testcount', methods = ['GET'])
def gettestcount():
    positive_count, negative_count, untested_count = RTR.testcounter()
    return {"positive_test": positive_count, "negative_test": negative_count, "untested": untested_count}



#--------------------------------------------------#
#                 INDEX PAGE                       #
#--------------------------------------------------#
# Index Page for APIs
@app.route('/')
def index():
    return {"Index Page":" ","TEAM DETAILS":"/api/getteam","RESET":"/api/reset","ZIPCODE ALERT":"/api/zipalertlist", "STATE ALERT":"/api/alertlist", "TOTAL TESTCOUNT":"/api/testcount", "PATIENT DETAILS":"/api/getpatient/<mrn>", "HOSPITAL DETAILS":"/api/gethospital/<id>" }

#--------------------------------------------------#
#                 ERROR HANDLER                    #
#--------------------------------------------------#
# Error Handler
@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)



if __name__ == '__main__':
    rabbit = Process(target = data_fetch)
    sending = Process(target = sendData)
    app.run(host = "0.0.0.0", debug = True, port = int('8088'))
