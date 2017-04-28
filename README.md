# Business Canada - East Or West

# [Link to website](http://www.eastorwest.ca)

# Execution instructions



## Data Collection
Run the two scripts below to download and decompress necessary data from web automatically
1. Change path for ROOT_PATH = "/home/.../ProjectData/" to download file in main function for the files below
2. Run  DataCollection/TorontoDataCollection.py to download data to data folder
3. Run  DataCollection/VancouverDataCollection.py to download data to data folder

## Data Cleaning
Run two files for Vancouver and Toronto to merge and clean data 
1. Change path for ROOT_PATH = "/home/.../ProjectData/" to download file in main function for the files below
2. Get an Yahoo api key to perform geo api operation from yahoo
3. SQLLITEFILE = ROOT_PATH +'latlongadd_db.sqlite'
4. Run DataClean/DataCleanPlatform.py to create necessary table in sqlite
5. Run DataClean/VancouverDataClean.py to merge data from various year and extract start and expiry date of business in a row
6. Run DataClean/TorontoDataClean.py to convert Address to latitude and longitude data (It might take a day or two two convert them all)

## Data Merging
Run DataMerging file DataMerging file by step

1. Start running by commenting all but step 1
2. Step2: Crowdsource match
3. Comment step 1 and 2 and run same file
4. Step 4 is crowsourcing for clustering
5. Finally run step 5 to clean data and ready for analysis

## Prediction
Run data_statistics.py to predict stability of business

# Web Server
## Run web server
To access prediction please run a server because cross origin supports http protocol.
From: Visualization/Website
Example: pythom -m SimpleHttpServer 



