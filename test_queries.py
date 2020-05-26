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
print ("#---------------------------------------------#")
# QUERY 1 - Count high speeds: Find the number of speeds > 100 in the data set.
#
# 
reading_count = db.loopdata.find({"speed": {"$gt": 100}}).count()
print("Count of 100+ speeds: " + str(reading_count))

print ("#---------------------------------------------#")
# QUERY 2 - Volume: Find the total volume for the station Foster NB for Sept 21, 2011.
#
# find station id for Foster NB
query = {"locationtext": "Foster NB"}
detectors = db.detectors.find_one(query)
station = detectors['stationid']
# query all loopdata for station id and for sept 21, 2011 - return volume 
readings = db.loopdata.find({"stationid": '1045', "date": '2011-09-15'},{"volume" : 1})
total = 0
for reading in readings:
    if reading['volume'] != '':
        # sum volumes
        total += int(reading['volume'])
print("Volume: " + str(total))



print ("#---------------------------------------------#\n")
#readings = db.loopdata.find()
#for reading in readings:
    #print(reading)