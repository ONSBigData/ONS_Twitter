"""
Description:
Author: Bence Komarniczky
Date:
Python version: 3.4
"""

from datetime import datetime

import ons_twitter.cluster as cl


start_time = datetime.now()

test_tweets = ["192.168.0.97:30000", "twitter", "tweets"]
mongo_address = ["192.168.0.82:27017", "twitter", "address"]

# if __name__ == "__main__":
# a = cl.cluster_all(test_tweets, mongo_address)
#     print(a)
#
#
# print(datetime.now() - start_time)

cl.cluster_one_chunk(test_tweets,
                     mongo_address,
                     1,
                     debug=True,
                     debug_user=128639001)
#
# tweets = pymongo.MongoClient(test_tweets[0])[test_tweets[1]][test_tweets[2]]
# cursor = tweets.find({"chunk_id": 1, "user_id": 128639001}, {"_id": 1, "user_id": 1, "tweet.coordinates": 1})
#
# tweets_by_user = {}
#
# for new_tweet_mongo in cursor:
# new_tweet = [new_tweet_mongo["_id"],
#                  new_tweet_mongo["user_id"],
#                  new_tweet_mongo["tweet"]["coordinates"]]
#
#     # insert into dictionary
#     try:
#         tweets_by_user[new_tweet_mongo["user_id"]].append(new_tweet)
#     except KeyError:
#         tweets_by_user[new_tweet_mongo["user_id"]] = [new_tweet]
#
#
# this_user = tweets_by_user[128639001]
# a = cl.distance_matrix(this_user)
# print(a)
#
#
# mask = [[x for x in range(len(this_user))],
#         np.arange(len(this_user), dtype=np.uint32)]
#
# c, d = cl.create_one_cluster(this_user, mask, a)
#
# print(c)
#
# print("\n\n")
#
# print(d)