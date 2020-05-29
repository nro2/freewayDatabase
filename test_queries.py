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
print ("\n###############################################")
#print ("#---------------------------------------------#\n")
# print all loopdata - for testing/verification
#readings = db.loopdata.find()
#for reading in readings:
    #print(reading)
    
print ("#---------------------------------------------#")
# QUERY 1 - Count high speeds: Find the number of speeds > 100 in the data set.
print("Query 1")
reading_count = db.loopdata.find({"speed": {"$gt": 100}}).count()
print("Count of 100+ speeds: " + str(reading_count))

print ("#---------------------------------------------#")
# QUERY 2 - Volume: Find the total volume for the station Foster NB for Sept 21, 2011.
print("Query 2")
# find station id for Foster NB
query = {"locationtext": "Foster NB"}
detectors = db.detectors.find_one(query)
station = detectors['stationid']
# query all loopdata for station id and for sept 21, 2011 - return volume 
readings = db.loopdata.find({"stationid": station, "date": '2011-09-15'},{"volume" : 1})
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

intvlList = []
counter = 0
totSpeeds = 0
avgSpeed = 0
intvlTime = 0
lastIntvl = False

#find stationid for "Foster NB"
query = {"locationtext": "Foster NB"}
detectors = db.detectors.find_one(query)
stationNum = detectors['stationid']

query2 = {"stationid": "1045"}  #***replace "1045" with 'stationNum'***
station = db.stations.find_one(query2)
length = station['length']

#query all loopdata for station id and for sept 21, 2011
readings = db.loopdata.find({"stationid": '1045', "date": '2011-09-15'}).sort("time")  #***replace '1045' with 'station' and '2011-09-22'***
for reading in readings:
    newIntvl = False
    lastIntvl = False
    timestamp = reading['time']
    minutes = timestamp[3:5]
    seconds = timestamp[6:8]
    print(reading)

    if(int(minutes) % 5 == 0) and seconds == '00':
        newIntvl = True
#        print("***newIntvl***")  #debug
    elif(int(minutes) % 5 == 4) and seconds == '40':
        lastIntvl = True
#        print("***lastIntvl***")  #debug

#    print("Minutes:", minutes)  #debug
#    print("Seconds:", seconds)  #debug
   
    if newIntvl:
        totSpeeds = 0
        counter = 0
        intvlTime = timestamp

    if reading['speed'] != '':
        totSpeeds += (int(reading['speed']) * int(reading['volume']))
        counter += int(reading['volume'])
#        print("TotSpeeds:", totSpeeds, "Counter:", counter)  #debug

    if lastIntvl:
        if counter > 0 and avgSpeed > 0:
            avgSpeed = totSpeeds / counter
            time = (float(length) / avgSpeed) * 3600
            intvlList.append(tuple((intvlTime, time)))

#make sure we capture the last reading
if not lastIntvl:
    if counter > 0 and avgSpeed > 0:
        avgSpeed = totSpeeds / counter
        time = (float(length) / avgSpeed) * 3600
        intvlList.append(tuple((intvlTime, time)))

print("\n Interval Time")
for i in range(len(intvlList)):
    print(intvlList[i])


print ("#---------------------------------------------#")
# QUERY 4 - Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM on September 22, 2011 for station Foster NB. 
# Report travel time in seconds.
print("Query 4")

print ("#---------------------------------------------#")
# QUERY 5 - Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM on September 22, 2011 for the I-205 NB freeway. 
# Report travel time in minutes.
print("Query 5")

print ("#---------------------------------------------#")
# QUERY 6 - Route Finding: Find a route from Johnson Creek to Columbia Blvd on I-205 NB using the upstream and downstream fields.
print("Query 6")


print ("#---------------------------------------------#")
print ("###############################################\n")
