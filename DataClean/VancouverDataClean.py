# Spark Usage
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from Lib import file, geo
import sqlite3
import csv

class VancouverDataClean:

    def __init__(self, rootpath, sqllite_path):
        self.conf = SparkConf().setAppName('Squeeze merge data')
        self.spark_context = SparkContext(conf=self.conf)
        self.sql_context = SQLContext(self.spark_context)
        self.rootpath = rootpath
        self.sqllite_path = sqllite_path

    def squeeze_business_data(self, input_file, output_file):
        input_file = self.rootpath+input_file
        output_file = self.rootpath+output_file

        df = self.sql_context.read.format('com.databricks.spark.csv').\
            options(header='true', inferschema='true').\
            load(input_file)

        df = df.orderBy('YearRecorded')
        df.registerTempTable("VancouverBusiness")

        self.squezzed_business = self.sql_context.sql("""
            SELECT BusinessName, min(IssuedDate) IssueDate,
                max(ExpiredDate) ExpiryDate,
                last(YearRecorded) Year,
                last(Status) LastStatus,
                CONCAT(BusinessType ,' ',COALESCE(BusinessSubType,'')) BusinessType,
                first(House) House,
                first(Street) Street,
                first(City) City,
                first(Province) Province,
                first(Country) Country,
                first(PostalCode) PostalCode,
                first(Latitude) Latitude,
                first(Longitude) Longitude,
                first(LocalArea) LocalArea
            FROM VancouverBusiness
            GROUP BY CONCAT(BusinessType ,' ',COALESCE(BusinessSubType,'')), BusinessName
        """)
        #startend_business.show()
        # This is meant to run only on single machine
        # This is the reason behind coalesce(1)
        self.squezzed_business.coalesce(1).write.\
            format('com.databricks.spark.csv').\
            options(header='true').save(output_file+"temp")

        # Merge all files into single file (assuming it's not running on hdfs)
        file.merge_files(output_file+"temp", output_file, "csv")
        file.delete_dir(output_file + "temp")

    def unknown_lat_long(self, input_file, output_file):
        """
        Get All unknown lat long data which has address
        :param input_file:
        :param output_file:
        :return:
        """
        input_file = self.rootpath + input_file
        output_file = self.rootpath + output_file

        df = self.sql_context.read.format('com.databricks.spark.csv'). \
            options(header='true', inferschema='true'). \
            load(input_file)

        df.registerTempTable('UniqueVancouverBusiness')

        all_blank_address = self.sql_context.sql("""
        SELECT House, Street, City, Province, Country, PostalCode, LocalArea
        FROM UniqueVancouverBusiness
        WHERE Latitude IS NULL
        AND (House IS NOT NULL OR
        Street IS NOT NULL OR
        City IS NOT NULL OR
        Province IS NOT NULL OR
        Country IS NOT NULL OR
        PostalCode IS NOT NULL)
        """)

        all_blank_address.coalesce(1).write.format('com.databricks.spark.csv')\
        .options(header='true').save(output_file+"temp")

        # Merge all files into single file (assuming it's not running on hdfs)
        file.merge_files(output_file + "temp", output_file, "csv")
        file.delete_dir(output_file + "temp")
        print("Total unknown address:"+str(all_blank_address.count()))

    def insert_address_sqllite(self, inputfile):
        """
        Inserts all unknown lat long address to sqlite database
        :param inputfile:
        :return:
        """

        inputfile = self.rootpath + inputfile

        # Connecting to the database file
        conn = sqlite3.connect(self.sqllite_path)
        c = conn.cursor()

        with open(inputfile, "r") as filepointer:
            csvpointer = csv.reader(filepointer)
            csvpointer.next()
            for csvline in csvpointer:
                house = csvline[0]
                street = csvline[1]
                city = csvline[2]
                province = csvline[3]
                country = csvline[4]
                postalcode = csvline[5]

                geo.insert_address_row_sqlite(c, house+" "+street,
                                              city+","+province,
                                              postalcode)
        conn.commit()
        conn.close()

    def update_sqllite_latlong(self, total_count):
        """
        Update sqllite database table with latitude and longitude
        :param total_count:
        :param sqlite_file:
        :return:
        """

        # Connecting to the database file
        conn = sqlite3.connect(self.sqllite_path)
        c = conn.cursor()

        counter = 0
        csvline = ['1']
        while counter < total_count:
            counter += 1
            c.execute(
                "SELECT HouseStreet, CityProvince, PostalCode FROM AddressMerged WHERE IsUpdated=0 LIMIT 1")
            csvline = c.fetchall()
            if len(csvline) > 0:
                csvline = list(csvline[0])
                housestreet = csvline[0]
                cityprovince = csvline[1]
                postalcode = csvline[2]
                lat, long = geo.latlong_from_yahoo(housestreet+" "+cityprovince+" "+postalcode)
                geo.update_latlong_sqllite(c, lat, long, housestreet,
                                           cityprovince, postalcode)
                print(lat, long, housestreet, cityprovince,  postalcode)
                conn.commit()
            else:
                break

        conn.close()
        return "Done"

    def reconcile_latitude_longitude(self, inputfile, outputfile):

        inputfile = self.rootpath + inputfile
        outputfile = self.rootpath + outputfile

        # Connecting to the database file
        conn = sqlite3.connect(self.sqllite_path)
        c = conn.cursor()

        f = csv.writer(open(outputfile, "w"))

        with open(inputfile, 'r') as filepointer:
            csvreader = csv.reader(filepointer)
            headerline = csvreader.next()
            #print(headerline)
            f.writerow(headerline)
            for csvline in csvreader:
                house = csvline[6]
                street = csvline[7]
                city = csvline[8]
                province = csvline[9]
                country = csvline[10]
                postalcode = csvline[11]
                latitude = csvline[12]
                longitude = csvline[13]
                if latitude == "" or longitude == "":
                    latitude, longitude = geo.find_latlong(c, house+" "+street,
                                                           city+","+province,
                                                           postalcode)
                    if latitude != 0 or longitude != 0:
                        csvline[12] = str(latitude)
                        csvline[13] = str(longitude)
                f.writerow(csvline)
                # print csvline
                # break
        conn.commit()
        conn.close()

    def get_necessary_fields(self, inputfile, outputfile):
        """
        Get fields necessary for data analysis
        :param inputfile:
        :param outputfile:
        :return:
        """
        inputfile = self.rootpath + inputfile
        outputfile = self.rootpath + outputfile

        f = csv.writer(open(outputfile, "w"))

        with open(inputfile, 'r') as filepointer:
            csvreader = csv.reader(filepointer)
            headerline = csvreader.next()
            #print(headerline)
            linehead = "DataSetOrigin,BusinessName,Status,IssuedDate,ExpiryDate,BusinessType,HouseStreet,CityProvince,PostalCode,Latitude,Longitude"
            f.writerow(linehead.split(","))

            for csvline in csvreader:
                businessname = csvline[0]
                issuedate = csvline[1]
                expirydate = csvline[2]
                status = csvline[4]
                businesstype = csvline[5]
                house = csvline[6]
                street = csvline[7]
                city = csvline[8]
                province = csvline[9]
                country = csvline[10]
                postalcode = csvline[11]
                latitude = csvline[12]
                longitude = csvline[13]

                f.writerow(["Van", businessname, status, issuedate,
                            expirydate, businesstype, house+" "+street,
                            city+","+province, postalcode, latitude, longitude])
                # print csvline
                # break

if __name__ == "__main__":

    ROOT_PATH = "/home/jay/BigData/SFUCourses/CMPT733/ProjectData/"
    SQLLITEFILE = ROOT_PATH +'latlongadd_db.sqlite'

    vandataclean = VancouverDataClean(ROOT_PATH+"Vancouver/", SQLLITEFILE)

    print("Squeezing vancouver business data from multi year.")
    # Squeezed vancouver business data from multi year
    vandataclean.squeeze_business_data("raw_merged_vancouver.csv",
                                       "sqeezed_vancouver_data.csv")

    print("Creating list for unknown latitude longitude for addresses")
    # List out all the business address that does not have any latitude longitude
    vandataclean.unknown_lat_long("sqeezed_vancouver_data.csv",
                                  "unknown_vancouver_latlong.csv")

    # print("Inserting Address to sqllite database table")
    # print("If it already exists, it will keep it")
    # # Add all the address to SQL lite database
    # vandataclean.insert_address_sqllite("unknown_vancouver_latlong.csv")
    #
    # print("Find address to latitude longitude for unknown address")
    # # Query from web service and fill lat long data into sqlite database
    # vandataclean.update_sqllite_latlong(1000)

    print("Creates final dataset with latitude and longitude data")
    # create new dataset
    vandataclean.reconcile_latitude_longitude("sqeezed_vancouver_data.csv",
                                              "sqeezed_vancouver_data_wll.csv")

    print("Creates final clean data with only necessary fields")
    # create new dataset
    vandataclean.get_necessary_fields("sqeezed_vancouver_data_wll.csv",
                                              "FINAL_VANCOUVER.csv")

