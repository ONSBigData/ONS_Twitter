"""
Description:
Author: Bence Komarniczky
Date:
Python version: 3.4
"""

import pymongo
from datetime import datetime

tweets = pymongo.MongoClient("192.168.0.99:30000")["twitter"]["tweets"]

fill_this = {}

# chunk 0
start_time = datetime.now()
print(start_time)

# for chunk_number in range(1000):
#     start_agg = datetime.now()
#     a = tweets.aggregate(
#         [{"$match": {"chunk_id": chunk_number, "cluster.type": "cluster", "cluster.address.classification.abbreviated": "R"}},
#          {"$group": {"_id": "$cluster.cluster_id", "user_id": {"$min": "$user_id"}, "count": {"$sum": 1}}},
#          {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
#          {"$group": {"_id": "$count", "number": {"$sum": 1}}}])
#
#     print(chunk_number, datetime.now() - start_agg)
#     for id in a["result"]:
#         try:
#             fill_this[id["_id"]] += id["number"]
#         except KeyError:
#             fill_this[id["_id"]] = id["number"]
#
#
#     print(fill_this)
#
# print(datetime.now() - start_time)




start_agg = datetime.now()
a = tweets.aggregate(
    [{"$match": {"chunk_id": {"$lt": 50}, "cluster.type": "cluster", "cluster.address.classification.abbreviated": "R"}},
     {"$group": {"_id": "$cluster.cluster_id", "user_id": {"$first": "$user_id"}, "count": {"$sum": 1}}},
     {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
     {"$group": {"_id": "$count", "number": {"$sum": 1}}}])

print(datetime.now() - start_agg)
for id in a["result"]:
    try:
        fill_this[id["_id"]] += id["number"]
    except KeyError:
        fill_this[id["_id"]] = id["number"]


print(fill_this)

print(datetime.now() - start_time)