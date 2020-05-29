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

intvlList = []
counter = 0
totSpeeds = 0
avgSpeed = 0
intvlTime = 0
lastIntvl = False

#find stationid for "Foster NB"
query = {"locationtext": "Foster NB"}
detectors = db.detectors.find_one(query)
stationNum = detectors['stationid'] #***replace "1045" with 'stationNum'***

query2 = {"stationid": stationNum}  
station = db.stations.find_one(query2)
length = station['length']
date = "2011-10-20"

#query all loopdata for station id and for sept 21, 2011
readings = db.loopdata.find({"stationid": stationNum, "date": date}).sort("time")  #***replace '1045' with 'station' and '2011-09-22'***
for reading in readings:
    newIntvl = False
    lastIntvl = False
    timestamp = reading['time']
    minutes = timestamp[3:5]
    seconds = timestamp[6:8]
    #print(reading)

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
        avgSpeed = totSpeeds / counter
        time = (float(length) / avgSpeed) * 3600
        intvlList.append(tuple((intvlTime, round(time, 4))))

#make sure we capture the last reading
if not lastIntvl:
    if counter > 0 and avgSpeed > 0:
        avgSpeed = totSpeeds / counter
        time = (float(length) / avgSpeed) * 3600
        intvlList.append(tuple((intvlTime, round(time, 4))))

print("\n", date, "  ", stationNum)
print("Time Interval,  Travel Time")
for intvl in intvlList:
    print(intvl)


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

<<<<<<< HEAD

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
=======
count = 0
speeds = []
average = 0

readings = db.loopdata.find({
    "$or":
    [
        {"stationid": stationID,
        "date": '2011-10-28',
        "time": {
            "$gte": '18:00:40-07',
            "$lte": '18:03:20-07'
        }},
        {"stationid": stationID,
        "date": '2011-10-28',
        "time": {
            "$gte": '17:42:30-07',
            "$lte": '17:43:20-07'
        }}
    ]
})

for reading in readings:
    if reading['speed'] is not '':
        speeds.append(reading['speed'])
        count += 1


average = (stationLength/(sum(speeds)/count)) * 3600
print(speeds)
print(count)
print(average, "Is the average travel time for Foster NB")
>>>>>>> Write query 4.


print ("#---------------------------------------------#")
# QUERY 5 - Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM on September 22, 2011 for the I-205 NB freeway. 
# Report travel time in minutes.
print("Query 5")

print ("#---------------------------------------------#")
# QUERY 6 - Route Finding: Find a route from Johnson Creek to Columbia Blvd on I-205 NB using the upstream and downstream fields.
print("Query 6")


print ("#---------------------------------------------#")
print ("###############################################\n")


