import urllib
import os


def download_torontodata(root_path, filename):
    """
    Download toronto csv
    :param download_folder:
    :return:
    """
    if not os.path.exists(root_path):
        os.makedirs(root_path)

    http_url = "http://opendata.toronto.ca/mls/business.licences/business.licences.csv"
    urllib.urlretrieve (http_url, root_path + filename)

    print("Tornto CSV downloaded.")


if __name__ == "__main__":
    ROOT_PATH = "/home/jay/BigData/SFUCourses/CMPT733/ProjectData/"
    download_torontodata(ROOT_PATH+"Toronto/", "business.licences.csv")