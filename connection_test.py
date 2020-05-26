# CS588 Final project
# Seth Seeman
# Spring 2020
#
# test db connection script

import pymongo, sys
from pprint import pprint

ip = sys.argv[1]

usr = "root"
psswrd = "super!secret"

client = pymongo.MongoClient("mongodb://" + usr + ":" + psswrd + "@" + ip + ":27017/")
db = client.freeway 
print ("\n#---------------------------------------------#")
# query 1
cols = db.list_collection_names()
print(cols)
print ("#---------------------------------------------#")
# query 2 - all detector ids less than 1350
query2 = {"detectorid": { "$lt": 1350 }}
detectors = db.detectors.find(query2)
for detector in detectors:
        pprint(detector)
print ("#---------------------------------------------#\n")

