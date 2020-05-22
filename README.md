# Covid-19 management system
The implementation of an application to support Covid-19 management for the state of Kentucky.

# Introduction
The project is an implementation of an application to support Covid-19 management for the state of Kentucky. The application takes the patient data as input from the RabbitMQ server and finds the neartest hospital for the patient. The goals of the application are compromised of the following:
* To read the incoming patient data, find the best fit hospital for the patient based on the patient state and zipcode. Then store this data ina NoSQL database.
* To implement an API for searching the location of the patient by using the patient mrn (medical record number).
* To implement an API which gives the number of available beds and total beds in a particular hospital.
* To implement an API for real time reporting of the alert zipcodes. (zipcode is in alert state if the number of patients from a zipcode doubles over a 15 second time interval).
* To implement an API for real time reporting of the state alert. (State is in alert state if more than 5 zipcodes are in alert state within the same time interval).
