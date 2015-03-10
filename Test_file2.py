"""
Description:
Author: Bence Komarniczky
Date:
Python version: 3.4
"""

from datetime import datetime

start_time = datetime.now()
# import pymongo
# from ons_twitter.data_formats import *
#
# test_twitter_mongo = pymongo.MongoClient("127.0.0.1:27017").test.tweets
#
# query = {"chunk_id": 0}
# my_cursor = test_twitter_mongo.find(query, {"_id": 1, "user_id": 1, "tweet.coordinates": 1})
#
# tweets_by_user = {}
# for a in my_cursor:
#     new_row = [a["_id"], a["user_id"], a["tweet"]["coordinates"]]
#     try:
#
#         tweets_by_user[a["user_id"]].append(new_row)
#     except KeyError:
#         tweets_by_user[a["user_id"]] = [new_row]
#
#
# for key in tweets_by_user.keys():
#     print(tweets_by_user[key])

import ons_twitter.cluster as cl

trial = [['A', 908190000, [373820, 805600]],
         ['B', 908190000, [373823, 805679]],
         ['C', 908190000, [373825, 805684]],
         ['D', 908190000, [373838, 805600]],
         ['E', 908190000, [373838, 805600]],
         ['F', 908190000, [12, 805600]],
         ['G', 908190000, [8, 805600]]
         ]
print(datetime.now() - start_time)
trial = trial * 1

import numpy as np


distance_array = cl.distance_matrix(trial, block_size=10)

print(distance_array)

remaining_mask = [[x for x in range(len(trial))],
                  np.array([x for x in range(len(trial))], dtype="int32")]
print(datetime.now() - start_time)

continue_clustering = True

all_clusters = []
while continue_clustering:
    new_cluster, remaining_mask = cl.create_one_cluster(trial, remaining_mask, distance_array)
    if new_cluster is not None:
        all_clusters.append(new_cluster)
    else:
        continue_clustering = False


for cluster in all_clusters:
    print(cluster)


print(datetime.now() - start_time)