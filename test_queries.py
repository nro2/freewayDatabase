# CS588 Final project
# Seth Seeman
# Spring 2020
#
# test db connection script

import pymongo, sys
from pprint import pprint
import pandas as pd

try:
    ip = "35.236.54.92"
    #ip = sys.argv[1]
except:
    print("Must provide DB IP-Address as first argument")
    exit(1)

usr = "root"
psswrd = "super!secret"

client = pymongo.MongoClient("mongodb://" + usr + ":" + psswrd + "@" + ip + ":27017/")
db = client.freeway
print ("\n###############################################")
#print ("#---------------------------------------------#\n")
# print all loopdata - for testing/verification
#query = {"stationid":'1047'}
#readings = db.loopdata.find(query)
#for reading in readings:
#    print(reading)
    
count = db.loopdata.count()
print("Total Readings: " + str(count))
count = db.detectors.count()
print("Total Detectors: " + str(count))
count = db.loopdata.distinct("detectorid")
print("Found Detectors: " + str(len(count)) + " " + str(count))
count = db.stations.count()
print("Total Stations: " + str(count))
count = db.loopdata.distinct("stationid")
print("Found Stations: " + str(len(count)) + " " + str(count))
count = db.highways.count()
print("Total Highways: " + str(count))
count = db.loopdata.distinct("highwayid")
print("Found Highways: " + str(len(count)) + " " + str(count) + '\n')
    
print ("#---------------------------------------------#")
# QUERY 1 - Count high speeds: Find the number of speeds > 100 in the data set.
print("Query 1")
reading_count = db.loopdata.count_documents({"speed": {"$gt": 100}})
print("Count of 100+ speeds: " + str(reading_count))

print ("#---------------------------------------------#")
# QUERY 2 - Volume: Find the total volume for the station Foster NB for Sept 21, 2011.
print("Query 2")
# find station id for Foster NB
query = {"locationtext": "Foster NB"}
detectors = db.detectors.find_one(query)
station =  detectors['stationid']
#rint(station)
date = '2011-10-20'
# query all loopdata for station id and for sept 21, 2011 - return volume 
readings = db.loopdata.find({"stationid": station, "date": date},{"volume" : 1})
total = 0
for reading in readings:
    if reading['volume'] != '':
        # sum volumes
        total += int(reading['volume'])
print("Volume: " + str(total))

print ("#---------------------------------------------#")
# QUERY 3 - Single-Day Station Travel Times: Find travel time for station Foster NB for 5-minute intervals for Sept 22, 2011. 
# Report travel time in seconds.
print("Query 3")


class bucket():
    def __init__(self, intvl):
        self.intvl = intvl
        self.spdSum = 0
        self.counter = 0

spdBuckets = []  #24hours x 12 interval buckets to accummulate speeds
intvlList = []  #list of all time intervals

#set up spdBuckets array
intvls = pd.timedelta_range(0, periods = 288, freq='5Min')
for i in intvls:
    intvl = str(i)[7:12]
    spdBuckets.append(bucket(intvl))

#find stationid for "Foster NB"
query = {"locationtext": "Foster NB"}
detectors = db.detectors.find_one(query)
stationNum = detectors['stationid']

query2 = {"stationid": stationNum}  
station = db.stations.find_one(query2)
length = station['length']
date = "2011-10-20"  #***replace with '2011-09-22'

cur = 0  #current bucket

#query all loopdata for station id and for sept 22, 2011
readings = db.loopdata.find({"stationid": stationNum, "date": date}).sort("time")
for reading in readings:
    timestamp = str(reading['time'])[:5]  #the timestamp, as read
    roundedTime = None

    #round timestamp to the nearest interval
    if int(timestamp[4]) >= 0 and int(timestamp[4]) < 5:
        roundedTime = timestamp[:4] + '0'
    else:
        roundedTime = timestamp[:4] + '5'

    if reading['speed'] != '':
        #find the correct bucket to add the speed to -- since timestamps are sorted, no need to start over from 0 each time
        while spdBuckets[cur].intvl != roundedTime:
            cur += 1

        #can't simply add speed and increment counter by 1; must add speed x volume to properly calculate avg
        spdBuckets[cur].spdSum += (int(reading['speed']) * int(reading['volume']))
        spdBuckets[cur].counter += int(reading['volume'])

#calculate averages for each bucket
for bucket in spdBuckets:
    if bucket.counter > 0:
        avgSpd = bucket.spdSum / bucket.counter
        time = (length / avgSpd) * 3600
        intvlList.append(tuple((bucket.intvl, round(time,4))))
    #if no volume recorded for current interval
    else:
        intvlList.append(tuple((bucket.intvl, 'no readings')))


#print intervals and times
print("\n", date, "  ", stationNum)
print("Interval,  Travel Time")
for intvl in intvlList:
    if intvl[1] != 'no readings':
        print(intvl[0], "     {:0.3f}".format(intvl[1]))
    else:    
        print(intvl[0], "    ", intvl[1])


print ("#---------------------------------------------#")
# QUERY 4 - Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM on September 22, 2011 for station Foster NB. 
# Report travel time in seconds.
print("Query 4")

location = {"locationtext": "Foster NB"}
detectors = db.detectors.find_one(location)
stationID = detectors['stationid']

station = {"stationid": stationID}  
stations = db.stations.find_one(station)
stationLength = stations['length']

window1Lower = '18:00:40-07'
window1Upper = '18:03:20-07'

window2Lower = '17:42:30-07'
window2Upper = '17:43:20-07'

def calculateAverage(windowLower, windowUpper, stationLength): 
    count = 0
    speeds = []

    readings = db.loopdata.find({
        "stationid": stationID,
        "date": '2011-10-28',
        "time": {
            "$gte": windowLower,
            "$lte": windowUpper
        }
    })

    for reading in readings:
        if reading['speed'] is not '':
            speeds.append(reading['speed'])
            count += 1

    return (stationLength/(sum(speeds)/count)) * 3600

average1 = calculateAverage(window1Lower, window1Upper, stationLength)
average2 = calculateAverage(window2Lower, window2Upper, stationLength)

print(average1, "Is the average travel time for Foster NB from ", window1Lower, "to", window1Upper)
print(average2, "Is the average travel time for Foster NB from ", window2Lower, "to", window2Upper)


print ("#---------------------------------------------#")
# QUERY 5 - Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM on September 22, 2011 for the I-205 NB freeway. 
# Report travel time in minutes.
print("Query 5")

print ("#---------------------------------------------#")
# QUERY 6 - Route Finding: Find a route from Johnson Creek to Columbia Blvd on I-205 NB using the upstream and downstream fields.
print("Query 6")

startLocation = {"locationtext": "Johnson Cr NB"}
detectors = db.stations.find_one(startLocation)
startStationID = detectors['stationid']

endLocation = {"locationtext": "Columbia to I-205 NB"}
detectors = db.stations.find_one(endLocation)
endStationID = detectors['stationid']

path = []

currentStation = startStationID

while (currentStation != endStationID) and (currentStation != 0):
    currentLocation = db.stations.find_one({"stationid": currentStation})
    path.append(currentLocation['locationtext'])
    currentStation = currentLocation['downstream']

currentLocation = db.stations.find_one({"stationid": currentStation})
path.append(currentLocation['locationtext'])

print("The path from 'Johnson Cr NB' to 'Columbia to I-205 NB' is:")
print(path)

print ("#---------------------------------------------#")
print ("###############################################\n")


