# This is a Python script for MySQL DB Operations.

import mysql.connector as connection
from logger import App_Logger
import json
import csv
import os

class DBOperations:

    def __init__(self):
        self.logger = App_Logger()

    def dbConnection(self):

        try:
            file = open("LogFile.log", "a+")
            mydb = connection.connect(host="localhost", user="root", passwd="root", use_pure=True)
            self.logger.log(file,"DB connection successful")
            print(mydb.is_connected())

        except Exception as e:
            file = open("LogFile.log", "a+")
            self.logger.log(file, "Error while connecting to database: %s" % e)
            mydb.close()
            print(str(e))

        return mydb

    def createdb(self, DatabaseName):

        try:
            mydb = self.dbConnection()
            file = open("LogFile.log", "a+")
            cursor = mydb.cursor()
            cursor.execute("DROP DATABASE IF EXISTS " + DatabaseName)
            cursor.execute("CREATE DATABASE " + DatabaseName)
            self.logger.log(file, "DB %s created successfully" % DatabaseName)
            print("database created")

        except Exception as e:
            file = open("LogFile.log", "a+")
            self.logger.log(file, "Error while creating database: %s" % e)
            mydb.close()
            print(str(e))

        return None

    def createTable(self, DatabaseName, column_names):

        try:
            mydb = self.dbConnection()
            cursor = mydb.cursor()
            cursor.execute("USE " + DatabaseName)
            cursor.execute('DROP TABLE IF EXISTS PMTable;')

            for key in column_names.keys():
                type = column_names[key]
                try:
                    cursor.execute('ALTER TABLE PMTable ADD {column_name} {dataType}'.format(column_name=key, dataType=type))
                except:
                    cursor.execute("CREATE TABLE PMTable ({column_name} {dataType})".format(column_name=key, dataType=type))

            file = open("LogFile.log", "a+")
            self.logger.log(file, "Table PMTable created successfully")
            print("database created")
            mydb.close()

        except Exception as e:
            file = open("LogFile.log", "a+")
            self.logger.log(file, "Error while creating Table: %s" % e)
            mydb.close()
            print(str(e))

        return None

    def insertIntoPMTable(self, DatabaseName):

        mydb = self.dbConnection()
        cursor = mydb.cursor()
        log_file = open("LogFile.log", "a+")
        try:
            cursor.execute("USE " + DatabaseName)
            with open("ai4i+2020+predictive+maintenance+dataset/ai4i2020.csv", 'r') as f:
                next(f)
                reader = csv.reader(f, delimiter=",")
                for line in reader:
                    cursor.execute('INSERT INTO PMTable VALUES {values}'.format(values=tuple(line)))
                self.logger.log(log_file, " ai4i2020.csv: File loaded successfully!!")
            mydb.commit()

        except Exception as e:
            self.logger.log(log_file, "Error while inserting data: %s " % e)
            log_file.close()
            cursor.close()
            raise e

        return None

    def selectingDatafromtableintocsv(self, Database):

        self.fileFromDb = 'FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("LogFile.log", 'a+')

        try:
            conn = self.dbConnection()
            sqlSelect = "SELECT *  FROM PMTable"
            cursor = conn.cursor()
            cursor.execute("USE " + Database)
            cursor.execute(sqlSelect)

            results = cursor.fetchall()

            #Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            #Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            raise e

        return None

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dbOperations = DBOperations()
    dbOperations.createdb("PMdb")
    schema = open("ai4i+2020+predictive+maintenance+dataset/schema.json", "r")
    js = json.load(schema)
    dbOperations.createTable("PMdb", js['ColName'])
    dbOperations.insertIntoPMTable("PMdb")
    dbOperations.selectingDatafromtableintocsv("PMdb")

