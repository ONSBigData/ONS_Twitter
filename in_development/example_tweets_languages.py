"""
Description: Get some examples of tweets.
Author: Bence Komarniczky
Date: 04/06/2015
Python version: 3.4
"""


import pymongo


db = pymongo.MongoClient("192.168.0.99:30000")["twitter"]["tweets"]

cursor = db.find({"chunk_id": {"$lt": 10}, "cluster.dominant": 1, "tweet.language": "es"},
                 {"tweet.text": 1, "tweet.language": 1})

for a in cursor:
    print("language: %s   text: %s" % (a["tweet"]["language"], a["tweet"]["text"]))
