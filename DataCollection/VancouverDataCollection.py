import urllib
import os
import datetime
import zipfile
import csv
from Lib import Stringer


class VancouverDataCollection:

    startyear = 1997
    endyear = 2017

    def __init__(self, rootpath):
        self.rootpath = rootpath

    def vancouver_filename(self, year):
        """
        Get filename for a year
        :param year:
        :return:
        """
        now = datetime.datetime.now()
        filename = "business_licences_csv.zip"
        if year != now.year:
            filename = str(year) + filename
        return filename

    def vancouver_yearfromfilename(self, filename):
        """
        Get year from filename
        :param filename:
        :return:
        """
        now = datetime.datetime.now()
        year = filename[:4]
        if Stringer.is_number(year):
            return int(year)
        else:
            return now.year

    def download_all_vancouverdata(self, ftp_url, download_folder,
                                   startyear, endyear):
        """
        Download all files from vancouver ftp
        :param ftp_url:
        :param download_folder:
        :param startyear:
        :param endyear:
        :return:
        """
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        for year in range(startyear, endyear + 1):
            urllib.urlretrieve(
                ftp_url + self.vancouver_filename(year),
                download_folder + self.vancouver_filename(year))

            urllib.urlcleanup()
            print("Vancouver Data: Year:" + str(year) + " is downloaded.")

    def merge_all_vancouver_csv(self, source_folder, merged_filename_path):
        """
        Merge all vancouver CSV file
        :param source_folder:
        :param merged_filename_path:
        :return:
        """
        csvwriter = csv.writer(open(merged_filename_path, 'w'))

        is_header_printed = False
        all_files = os.listdir(source_folder)
        for filename in all_files:
            fileyear = self.vancouver_yearfromfilename(filename)
            with open(source_folder + filename, 'r') as csvfile:
                csvreader = csv.reader(csvfile)
                header = csvreader.next()
                # Print header only for the first time
                if is_header_printed == False:
                    header.append("YearRecorded")
                    csvwriter.writerow(header)
                    is_header_printed = True

                for row in csvreader:
                    if len(row) > 1:
                        row = [x.replace("\"", "") for x in row]
                        row.append(fileyear)
                        csvwriter.writerow(row)
            print("Merged year:"+str(fileyear))

    def unzip_files(self, source_folder, destination_folder):
        """
        Unzip vancouver zip files
        :param source_folder:
        :param destination_folder:
        :return:
        """
        all_zip_files = os.listdir(source_folder)

        for zip_file in all_zip_files:
            zip_ref = zipfile.ZipFile(source_folder + zip_file, 'r')
            zip_ref.extractall(destination_folder)
            zip_ref.close()
            print("Unzipped vancouver data, Filename:" + zip_file)

        print("All vancouver files unzipped at:" + destination_folder)

    def perform_data_collection(self, fromstepno=1):
        """
        Perform all data collection
        :param rootdir:
        :param stepno:
        :return:
        """
        vacouver_ftp_url = "ftp://webftp.vancouver.ca/OpenData/csv/"
        if fromstepno <= 1:
            # Step 1: Download vancouver data
            self.download_all_vancouverdata(vacouver_ftp_url,
                                            self.rootpath+"zip/", self.startyear,
                                            self.endyear)

        if fromstepno <= 2:
            # Step 2: Unzip vancouver data
            self.unzip_files(self.rootpath+"zip/",
                             ROOT_PATH+"csv/")

        if fromstepno <= 3:
            # Step 3: Merge all vancouver data with year at the end as YearRecorded
            self.merge_all_vancouver_csv(self.rootpath+"csv/",
                                         self.rootpath+"raw_merged_vancouver.csv")


if __name__ == "__main__":
    ROOT_PATH = "/home/jay/BigData/SFUCourses/CMPT733/ProjectData/"
    vandata = VancouverDataCollection(ROOT_PATH+"Vancouver/")
    vandata.perform_data_collection(3)