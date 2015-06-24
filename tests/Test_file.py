"""
Description:
Author: Bence Komarniczky
Date:
Python version: 3.4
"""

import pymongo
from ons_twitter.data_import import *
from ons_twitter.data_formats import *


create_partition_csv("data/input/Tweets_Apr_Oct_test_subset0.csv",
                     output_folder="data/input/chunk_test/")



# mongo_address = (("127.0.0.1:27017", "twitter", "address"),
#                  ("192.168.0.82:27017", "twitter", "address"),
#                  ("192.168.0.62:28001", "twitter", "address"),
#                  ("192.168.0.87:28000", "twitter", "address"),
#                  ("192.168.0.97:28002", "twitter", "address"),
#                  ("192.168.0.97:28003", "twitter", "address"))
#
# # mongo_address = ("192.168.0.82:27017", "twitter", "address")
# original_file = "C:/Users/ONS-BIG-DATA/Documents/TWITTER/twitter/data/input/Tweets_Apr_Oct.csv"
# create_partition_csv(original_file, "data/input/chunk_test/",  num_rows=10000, chunk_size=1000)
#
# test_twitter_mongo = ("127.0.0.1:27017", "test", "tweets")
#
# if __name__ == "__main__":
#     a = import_file("data/input/chunk_test",
#                    mongo_connection=test_twitter_mongo,
#                    mongo_address=mongo_address)