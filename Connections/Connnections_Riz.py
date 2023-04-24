import pymongo
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import mysql.connector
from mysql.connector.errors import OperationalError
import time
import urllib

class DBConnections:
    def Connection_mongo():
        try:
            client = pymongo.MongoClient('mongodb://localhost:27017'
                     #username='user',
                     #password='password',
                     #authSource='the_database',
                     #authMechanism='SCRAM-SHA-256'
                     ) 
            #mongo_uri = "mongodb://dap2023:" + urllib.parse.quote("password@123") + "@cluster0.gbbogr4.mongodb.net"
            #client2 = pymongo.MongoClient(mongo_uri)
            return client
        
        except pymongo.errors.ConnectionFailure as e:
            print ("Could not connect to server: %s", e)
            print ("\nTrying to reconnect the server in 5 seconds...")
            time.sleep(5)

    def Connection_Mysql(password):
        try:
            # Creating connection object
            mydb = mysql.connector.connect(
                host = "localhost",
                user = "root",
                password = password
            )
            print("\n _____________ Connected to MYSQL DB _____________________")
            return mydb

        except OperationalError as e:
            print ("Could not connect to server: %s ", e)
            print ("\nTrying to reconnect the server in 5 seconds...")
            time.sleep(5)