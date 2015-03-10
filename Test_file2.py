"""
Description:
Author: Bence Komarniczky
Date:
Python version: 3.4
"""

from datetime import datetime
import ons_twitter.cluster as cl

start_time = datetime.now()
import pymongo
from ons_twitter.data_formats import *

test_twitter_mongo = pymongo.MongoClient("127.0.0.1:27017").test.tweets

tweets_by_user = cl.create_dictionary_for_chunk(test_twitter_mongo, 0)

for key in tweets_by_user.keys():
    print(tweets_by_user[key])


#
# trial = [['A', 908190000, [373820, 805600]],
#          ['B', 908190000, [373823, 805679]],
#          ['C', 908190000, [373825, 805684]],
#          ['D', 908190000, [373838, 805600]],
#          ['E', 908190000, [373838, 805600]],
#          ['F', 908190000, [12, 805600]],
#          ['G', 908190000, [8, 805600]]
#          ]
# print(datetime.now() - start_time)
# trial = trial * 1
#
# import numpy as np
#
#
# distance_array = cl.distance_matrix(trial, block_size=10)
#
# print(distance_array)
#
# remaining_mask = [[x for x in range(len(trial))],
#                   np.array([x for x in range(len(trial))], dtype="int32")]
# print(datetime.now() - start_time)
#
# continue_clustering = True
#
# all_clusters = []
# while continue_clustering:
#     new_cluster, remaining_mask = cl.create_one_cluster(trial, remaining_mask, distance_array)
#     if new_cluster is not None:
#         all_clusters.append(new_cluster)
#     else:
#         continue_clustering = False
#
#
# for cluster in all_clusters:
#     print(cluster)
#
#
# print(datetime.now() - start_time)