# CS588 Final project
# Seth Seeman
# Spring 2020
#
#

import pymongo, sys

ip = sys.argv[1]
usr = "root"
psswrd = "super!secret"

client = pymongo.MongoClient(
        "mongodb://" + usr + ":" + psswrd + "@" + ip + "/test?retryWrites=true&w=majority")
db = client.freeway 
