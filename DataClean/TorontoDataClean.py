from Lib import file, geo, Stringer
import sqlite3
import csv

class TorontoDataClean:

    def __init__(self, rootpath, sqllite_path):
        self.rootpath = rootpath
        self.sqllite_path = sqllite_path

    def get_necessary_fields_and_address_insert(self, source_file, dest_file):

        # Connecting to the database file
        conn = sqlite3.connect(self.sqllite_path)
        c = conn.cursor()

        source_file = self.rootpath + source_file
        dest_file = self.rootpath + dest_file

        f = csv.writer(open(dest_file, "w"))
        with open(source_file, "r") as filepointer:
            csvpointer = csv.reader(filepointer)
            csvpointer.next()
            linehead = "BusinessName,Status,IssuedDate,ExpiryDate,BusinessType,HouseStreet,CityProvince,PostalCode,Latitude,Longitude"
            f.writerow(linehead.split(","))
            for row in csvpointer:
                businessname = Stringer.remove_quote(row[4] + " " + row[1])
                issuedate = Stringer.remove_quote(row[3])
                expirydate = Stringer.remove_quote(row[15])
                business_type = Stringer.remove_quote(row[0])
                housestreet = Stringer.remove_quote(row[7])
                cityprovince = Stringer.remove_quote(row[8])
                postalcode = Stringer.remove_quote(row[9])
                status = "Closed"
                if expirydate == "":
                    status = "Open"
                    expirydate = "31-DEC-2017"
                lon = ""
                lat = ""

                f.writerow(
                    [businessname, status, issuedate, expirydate,
                     business_type, housestreet,
                     cityprovince, postalcode,
                     lat, lon])
                geo.insert_address_row_sqlite(c, housestreet, cityprovince, postalcode)

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
            headerline.insert(0, "DataSetOrigin")
            f.writerow(headerline)
            for csvline in csvreader:
                housestreet = csvline[5]
                cityprovince = csvline[6]
                postalcode = csvline[7]
                latitude = csvline[8]
                longitude = csvline[9]
                if latitude == "" or longitude == "":
                    latitude, longitude = geo.find_latlong(c, housestreet,
                                                           cityprovince,
                                                           postalcode)
                    if latitude != 0 or longitude != 0:
                        csvline[8] = str(latitude)
                        csvline[9] = str(longitude)
                csvline.insert(0, "Tor")
                f.writerow(csvline)
                # print csvline
                # break
        conn.close()

if __name__ == "__main__":

    ROOT_PATH = "/home/jay/BigData/SFUCourses/CMPT733/ProjectData/"
    SQLLITEFILE = ROOT_PATH +'latlongadd_db.sqlite'

    tordataclean = TorontoDataClean(ROOT_PATH+"Toronto/", SQLLITEFILE)

    # print("Getting formatted data and inserting into sqllite database")
    # # Squeezed vancouver business data from multi year
    # tordataclean.get_necessary_fields_and_address_insert("business.licences.csv",
    #                                    "clean_woll_clean.csv")
    #
    # print("Updating by address to lat long conversion")
    # # List out all the business address that does not have any latitude longitude
    # tordataclean.update_sqllite_latlong("clean_woll_clean.csv")

    print("Creates final dataset with latitude and longitude data")
    # create new dataset
    tordataclean.reconcile_latitude_longitude("clean_woll_clean.csv",
                                              "FINAL_TORONTO.csv")

