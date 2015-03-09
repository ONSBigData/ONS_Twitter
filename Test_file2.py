"""
Description:
Author: Bence Komarniczky
Date:
Python version: 3.4
"""
import pymongo
from ons_twitter.data_formats import *

test_twitter_mongo = pymongo.MongoClient("127.0.0.1:27017").test.tweets

query = {"chunk_id": 0}
my_cursor = test_twitter_mongo.find(query, {"_id": 1, "user_id": 1, "tweet.coordinates": 1})

tweets_by_user = {}
for a in my_cursor:
    new_row = [a["_id"], a["user_id"], a["tweet"]["coordinates"]]
    try:

        tweets_by_user[a["user_id"]].append(new_row)
    except KeyError:
        tweets_by_user[a["user_id"]] = [new_row]


for key in tweets_by_user.keys():
    print(tweets_by_user[key])

trial = [['908190000_1396375708', 908190000, [373820, 805674]],
         ['908190000_1396375319', 908190000, [373823, 805679]],
         ['908190000_1396375534', 908190000, [373825, 805684]]]

import numpy as np

z = np.array([complex(c[2][0], c[2][1]) for c in trial])

m, n = np.meshgrid(z, z)

out = abs(m - n)