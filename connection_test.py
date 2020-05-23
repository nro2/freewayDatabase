# CS588 Final project
# Seth Seeman
# Spring 2020
#
# test db connection script

import pymongo, sys

ip = sys.argv[1]

usr = "root"
psswrd = "super!secret"

client = pymongo.MongoClient("mongodb://" + usr + ":" + psswrd + "@" + ip + ":27017/")
db = client.freeway 
x = col = db.list_collection_names()
print(x)