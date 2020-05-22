from io import open
import pyorient
import json
import csv
import logging
import errno


#--------------------------------------------------#
#            READ PATIENTS FROM RABBITMQ           #
#--------------------------------------------------#
def readpatients(mrn,zipcode,patient_status_code):
    #database name
    dbname = "covid19-ky"

    #database login is root by default
    login = "root"

    #dtabase password
    password = "rootpwd"

    #creating client to connect to local orientdb
    client = pyorient.OrientDB("localhost",2424)
    session_id = client.connect(login,password)


    #open the database
    client.db_open(dbname,login,password)

    location_code = -1
    client.command("CREATE VERTEX patient SET mrn = '" + mrn + "', zipcode = '" + zipcode + "', patient_status_code = '" + patient_status_code + "'")
    location_code = bestFitHospital(client,mrn,zipcode,patient_status_code)
    client.command("UPDATE patient SET location_code = '" + location_code + "' WHERE mrn = '" + mrn + "'")

    if int(patient_status_code) == 3 or int(patient_status_code) == 5 or int(patient_status_code) == 6:
        z = client.query("SELECT zipcode FROM zipcodes WHERE zipcode = '" + zipcode +"'")
        if len(z) != 0:
            client.command("UPDATE zipcodes INCREMENT positive_count = 1 WHERE zipcode = '" + zipcode + "'")
        else:
            client.command("CREATE VERTEX zipcodes SET zipcode = '" + zipcode + "', positive_count = 1, last_count = 0,"
                                                                                " zipalertlist = ' ', statealert = 0")



#--------------------------------------------------#
#       TO FIND NEAREST HOSPITAL FOR PATIENTS      #
#--------------------------------------------------#
def nearestZips(client, patient_zip, patient_status_code):
    #assign file path
    filepath = 'kyzipdistance.csv'
    hospitalId = '0'
    locationId = '0'

    #open the file and read it
    with open(filepath,encoding='utf-8-sig') as infile:
        for line in infile:
            line = line.strip()
            line = line.replace('"', '')
            lineSplit = line.split(",")
            zip_from = lineSplit[0]
            if zip_from == patient_zip:
                zip_to = lineSplit[1]
                #if patient is critical, transfer him to a hospital with a hospital level
                if int(patient_status_code) == 6:
                    hospitalinfo = client.query("SELECT * FROM hospital WHERE hospital_level != 'NULL' AND hospital_level !='NOT AVAILABLE'AND avl_beds > 0")
                    number_of_hospitals = len(hospitalinfo)
                    for key in range(number_of_hospitals-1):
                        hospitalId = str(hospitalinfo[key].oRecordData['id'])
                        hospital_zipcode = str(hospitalinfo[key].oRecordData['hospital_zipcode'])
                        if hospital_zipcode == zip_to:
                            locationId = hospitalId
                            return locationId
                #assign non-critical patients who require hospital to nearest hospital
                else:
                    hospitalinfo = client.query("SELECT * FROM hospital WHERE avl_beds > 0")
                    number_of_hospitals = len(hospitalinfo)
                    for key in range(number_of_hospitals-1):
                        hospitalId = str(hospitalinfo[key].oRecordData['id'])
                        hospital_zipcode = str(hospitalinfo[key].oRecordData['hospital_zipcode'])
                        if hospital_zipcode == zip_to:
                            locationId = hospitalId
                            return locationId
    return locationId


#--------------------------------------------------#
#       TO FIND IF PATIENT NEEDS A HOSPITAL        #
#--------------------------------------------------#
def bestFitHospital(client,mrn,zipcode,patient_status_code):
    location_code = '-1'
    #check if patient needs to be assigned a hospital
    if int(patient_status_code) == 3 or int(patient_status_code) == 5 or int(patient_status_code) == 6:
        data2 = client.command("SELECT * FROM hospital WHERE hospital_zipcode = '" + zipcode + "' AND avl_beds > 0")
        #if hospital zipcode and patient zipcode is same, assign a hospital and decrease the beds
        if(len(data2) == 1):
            hospital_zipcode = data2[0]
            location_code  = str(hospital_zipcode.oRecordData['id'])
            client.command("UPDATE hospital INCREMENT avl_beds = -1 WHERE id = '" + location_code + "'")
        #if no zipcodes match directly, find a hospital with nearest zipcode
        else:
            location_code = str(nearestZips(client, zipcode, patient_status_code))
            client.command("UPDATE hospital INCREMENT avl_beds = -1 WHERE id = '" + location_code + "'")
    #assign location code as -1 for patients that shouldn't have an assignment
    elif int(patient_status_code) == 0 or int(patient_status_code) == 1:
        location_code = '-1'
    #assign location code as 0 for home assignment
    else:
        location_code = '0'
    return location_code


#--------------------------------------------------#
#    DIRECT OUTPUT ACCORDING TO OF2 REQUIREMENTS   #
#--------------------------------------------------#
def OF2(mrn):
    # database name
    dbname = "covid19-ky"
    # database login is root by default
    login = "root"
    # database password, set by docker param
    password = "rootpwd"

    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    # open the database we are interested in
    client.db_open(dbname, login, password)

    #query the right location using the location_code from patient class and return it
    data = client.query("SELECT location_code FROM patient WHERE mrn = '" + mrn + "'")
    client.close()
    if len(data) != 0:
        location_code = str(data[0].oRecordData['location_code'])
        return location_code
    else:
        return "Invalid mrn"


#--------------------------------------------------#
#    DIRECT OUTPUT ACCORDING TO OF3 REQUIREMENTS   #
#--------------------------------------------------#
def OF3(id):
    # database name
    dbname = "covid19-ky"
    # database login is root by default
    login = "root"
    # database password, set by docker param
    password = "rootpwd"

    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    # open the database we are interested in
    client.db_open(dbname, login, password)

    #query the hospital data for the required hospital and return it
    data = client.query("SELECT * FROM hospital WHERE id = '" + id + "'")
    client.close()
    if len(data) == 1:
        beds = str(data[0].oRecordData['beds'])
        avl_beds = str(data[0].oRecordData['avl_beds'])
        hospital_zipcode = str(data[0].oRecordData['hospital_zipcode'])
        name = str(data[0].oRecordData['name'])
        hospital_data = [name, beds, avl_beds, hospital_zipcode]
        return hospital_data
    else:
        return ["Invalid hospital", '0','0','0']
