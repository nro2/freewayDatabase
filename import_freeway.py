# CS588 Final project
# Seth Seeman
# Spring 2020
#
# script to import all freeway data csv files to new mongoDB database

import pymongo, sys, csv

try:
        ip = "35.236.54.92"
        #ip = sys.argv[1]
except:
        print("Must provide DB IP-Address as first argument")
        exit(1)
try:
        data_dir = "sample_data/"
        #data_dir = sys.argv[2]
except:
        print("Must provide data directory as second argument")
        exit(1)

usr = "root"
psswrd = "super!secret"

client = pymongo.MongoClient("mongodb://" + usr + ":" + psswrd + "@" + ip + ":27017/")
db = client["freeway"] 

detector_file = data_dir + "/detectors.csv"
station_file = data_dir + "/stations.csv"
highway_file = data_dir + "/highways.csv"
loopdata_file = data_dir + "/freeway_loopdata.csv"

detectors = db["detectors"]

with open(detector_file, 'r') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(csv_reader)       # skips first line (column headers)
        for line in csv_reader:
                #detectorid = line[0]
                #highwayid = line[1]
                #milepost = line[2]
                #locationtext = line[3]
                #detectorclass = line[4]
                #lanenumber = line[5]
                #stationid = line[6]
                
                detector_dict = { "detectorid" : line[0],
                                "highwayid" : line[1],
                                "milepost" : line[2],
                                "locationtext" : line[3],
                                "detectorclass" : line[4],
                                "lanenumber" : line[5],
                                "stationid" : line[6]
                }
                result = detectors.insert_one(detector_dict)
                print(result)