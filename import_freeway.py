# CS588 Final project
# Seth Seeman
# Spring 2020
#
# script to import all freeway data csv files to new mongoDB database

import pymongo, sys, os, csv
from pprint import pprint


#-------------------------------------------------------#
def read_input():
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
        return ip, data_dir

#-------------------------------------------------------#
def db_connect(ip):
        user = "root"         #str(os.environ.get('USER'))
        password = "super!secret"     #str(os.environ.get('PASSWORD'))

        client = pymongo.MongoClient("mongodb://" + user + ":" + password + "@" + ip + ":27017/")
        db = client["freeway"]
        return db

#-------------------------------------------------------#
def import_highways(db, highway_file):
        
        # delete and recreate collection (OVERWRITE)
        db.highways.drop()
        highways = db["highways"]
        
        # read csv and write to MongoDB
        with open(highway_file, 'r') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                next(csv_reader)       # skips first line (column headers)
                for line in csv_reader:
                        hwref = db.highways.ObjectId()
                        print(hwref)
                        highway_dict = { "highwayid" : line[0],
                                        "shortdirection" : line[1],
                                        "direction" : line[2],
                                        "highwayname" : line[3]
                        }
                        result = highways.insert_one(highway_dict)
        
#        highways = db.highways.find()
#        for highway in highways:
#                print(highway)

#-------------------------------------------------------#
def import_stations(db, station_file):
        
        # delete and recreate collection (OVERWRITE)
        db.staions.drop()
        stations = db["stations"]
        
        # read csv and write to MongoDB
        with open(station_file, 'r') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                next(csv_reader)       # skips first line (column headers)
                for line in csv_reader:
                        highway = dict(db.highways.find({"highwayid": line[1]})[0])
                        hw_ref = highway['_id']
                        
                        station_dict = { "stationid" : line[0],
                                        "highwayid" : line[1],
                                        "highwayref": str(hw_ref),
                                        "milepost" : line[2],
                                        "locationtext" : line[3],
                                        "upstream" : line[4],
                                        "downstream" : line[5],
                                        "stationclass" : line[6],
                                        "numberlanes" : line[7],
                                        "latlon" : line[8],
                                        "length" : line[9]
                        }
                        result = stations.insert_one(station_dict)
                        
 #       stations = db.stations.find()
 #       for station in stations:
 #               pprint(station)
                        
#-------------------------------------------------------#
def import_detectors(db, detector_file):
        # delete and recreate collection (OVERWRITE)
        db.detectors.drop()
        detectors = db["detectors"]

        with open(detector_file, 'r') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                next(csv_reader)       # skips first line (column headers)
                for line in csv_reader:
                        highway = dict(db.highways.find({"highwayid": line[1]})[0])
                        hw_ref = highway['_id']
                        station = dict(db.stations.find({"stationid": line[6]})[0])
                        st_ref = station['_id']
                        
                        detector_dict = { "detectorid" : line[0],
                                        "highwayid" : line[1],
                                        "highwayref": str(hw_ref), 
                                        "milepost" : line[2],
                                        "locationtext" : line[3],
                                        "detectorclass" : line[4],
                                        "lanenumber" : line[5],
                                        "stationid" : line[6],
                                        "stationref": str(st_ref)
                        }
                        result = detectors.insert_one(detector_dict)

 #       detectors = db.detectors.find()
 #       for detector in detectors:
 #               print(detector)
 
 #-------------------------------------------------------#
def import_loopdata(db, loopdata_file):
        # delete and recreate collection (OVERWRITE)
        db.loopdata.drop()
        loopdata = db["loopdata"]

        with open(loopdata_file, 'r') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                next(csv_reader)       # skips first line (column headers)
                
                i = 0
                for line in csv_reader:
                        detector = dict(db.detectors.find({"detectorid": line[0]})[0])
                        hw_ref = detector['highwayref']
                        st_ref = detector['stationref']
                        
                        loopdata_dict = { "detectorid" : line[0],
                                        "starttime" : line[1],
                                        "volume": line[2], 
                                        "speed" : line[3],
                                        "occupancy" : line[4],
                                        "status" : line[5],
                                        "dqflags" : line[6],
                                        "stationid": st_ref,
                                        "highwayid": hw_ref
                        }
                        result = loopdata.insert_one(loopdata_dict)
                        i += 1
                        print(i)

        loopdata = db.loopdata.find()
        for reading in loopdata:
                print(reading)

#---------------------MAIN------------------------------#
def main():
        ip, data_dir = read_input()
        db = db_connect(ip)
       
        detector_file = data_dir + "/detectors.csv"
        station_file = data_dir + "/stations.csv"
        highway_file = data_dir + "/highways.csv"
        loopdata_file = data_dir + "/freeway100k_sample.csv"
  
        import_highways(db, highway_file)
        import_stations(db, station_file)
        import_detectors(db, detector_file)
        import_loopdata(db, loopdata_file)
  

if __name__ == "__main__":
    main()
