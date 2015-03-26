__author__ = 'Bence Komarniczky'
"""
Purpose:

Date:
"""

import csv

import pymongo


db = pymongo.MongoClient("192.168.0.97:30000")["twitter"]["tweets"]

cursor = db.find({"chunk_id": 0}, {"_id": 1})

with open("data/output/id_chunk_000.csv", 'w', newline="\n") as outcsv:
    my_writer = csv.writer(outcsv, delimiter=",", quoting=csv.QUOTE_NONNUMERIC)
    for tweet in cursor:
        my_writer.writerow([tweet["_id"]])
