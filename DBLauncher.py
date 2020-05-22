from io import open
import pyorient
import json
import csv
import logging
import errno


#--------------------------------------------------#
#               RESET THE DATABASE                 #
#--------------------------------------------------#
def reset_db():

    #database name
    dbname = "covid19-ky"

    #database login is root by default
    login = "root"

    #dtabase password
    password = "rootpwd"

    #creating client to connect to local orientdb
    client = pyorient.OrientDB("localhost",2424)
    session_id = client.connect(login,password)

    try:
        # To drop the database if it exists.
        if client.db_exists(dbname):
            client.db_drop(dbname)
        # create new database
        client.db_create(dbname,
                         pyorient.DB_TYPE_GRAPH,
                         pyorient.STORAGE_TYPE_PLOCAL)
        logging.info(dbname,"Database Created")
        statusCode = 1
    except pyorient.PyOrientException as err:
        logging.critical("Failed to Create", dbname, "DB:", err)
        statusCode = 0
    return statusCode


#--------------------------------------------------#
#                LOAD THE DATABASE                 #
#--------------------------------------------------#
def load_db():

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

    client.command("CREATE CLASS patient extends V")
    client.command("CREATE PROPERTY patient.first_name String")
    client.command("CREATE PROPERTY patient.last_name String")
    client.command("CREATE PROPERTY patient.mrn String")
    client.command("CREATE PROPERTY patient.zipcode integer")
    client.command("CREATE PROPERTY patient.patient_status_code integer")
    client.command("CREATE PROPERTY patient.location_code integer")

    client.command("CREATE CLASS zipcodes extends V")
    client.command("CREATE PROPERTY zipcodes.zipcode Integer")
    client.command("CREATE PROPERTY zipcodes.positive_count Integer")
    client.command("CREATE PROPERTY zipcodes.last_count Integer")
    client.command("CREATE PROPERTY zipcodes.zipalertlist String")
    client.command("CREATE PROPERTY zipcodes.statealert Integer")
    client.close()


#--------------------------------------------------#
#               READ HOSPITAL DATA                 #
#--------------------------------------------------#
def read_hospitaldata():

    filepath = 'hospitals.csv'
    data = {}
    #opening the hospitals file to access hospital data
    with open(filepath, encoding = 'utf-8-sig') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            identifier = row['ID']
            data[identifier] = row
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

    client.command("CREATE CLASS hospital extends V")
    client.command("CREATE PROPERTY hospital.name String")
    client.command("CREATE PROPERTY hospital.id Integer")
    client.command("CREATE PROPERTY hospital.address String")
    client.command("CREATE PROPERTY hospital.hospital_zipcode Integer")
    client.command("CREATE PROPERTY hospital.hospital_level String")
    client.command("CREATE PROPERTY hospital.beds Integer")
    client.command("CREATE PROPERTY hospital.state String")
    client.command("CREATE PROPERTY hospital.avl_beds Integer")


    for key in data:
        name = data.get(key).get("NAME")
        id = data.get(key).get("ID")
        address = data.get(key).get("ADDRESS")
        city = data.get(key).get("CITY")
        state = "KY"
        hospital_zipcode = data.get(key).get("ZIP")
        hospital_level = data.get(key).get("TRAUMA")
        beds = data.get(key).get("BEDS")
        avl_beds = data.get(key).get("BEDS")
        client.command("CREATE VERTEX hospital SET id = '" + id + "', name = '" + name + "', address = '" + address + "', city = '" + city + "', state = '" + state + "', hospital_zipcode = '" + hospital_zipcode + "', hospital_level = '" + hospital_level + "', beds = '" + beds + "', avl_beds = '" + avl_beds + "'")
    client.close()


#--------------------------------------------------#
#                  RESET DATA                      #
#--------------------------------------------------#
def resetRecords():
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
    client.command("DELETE VERTEX patient")
    client.command("UPDATE hospital SET avl_beds = beds")
    client.command("DELETE VERTEX zipcodes")
    client.close()
    return '1'
