"""
Description:
Author: Bence Komarniczky
Date:
Python version: 3.4
"""

import pymongo
from ons_twitter.data_import import *
from ons_twitter.data_formats import *

start_time = datetime.now()
mongo_address = ("192.168.0.82:27017", "twitter", "address")
# original_file = "C:/Users/ONS-BIG-DATA/Documents/TWITTER/twitter/data/input/Tweets_Apr_Oct.csv"
# create_test_csv(original_file, num_rows=10000, chunk_size=0)

test_file = "C:/Users/ONS-BIG-DATA/Documents/TWITTER/twitter/data/input/Tweets_Apr_Oct_test_subset0.csv"

test_twitter_mongo = ("192.168.0.97:30000", "twitter", "tweets")

a = import_csv("data/input/chunk_test",
           mongo_connection=test_twitter_mongo,
           mongo_address=mongo_address)

b = [(filename, test_twitter_mongo, mongo_address, False, False, None, 0) for filename in a]


if __name__ == "__main__":
    results = Parallel(n_jobs=-1)(delayed(import_one_csv)(filename,
                                                          test_twitter_mongo,
                                                          mongo_address,
                                                          False,
                                                          False,
                                                          None,
                                                          0) for filename in a)
    print(results)



# get_diagnostics = import_one_csv(original_file,
#                                  mongo_connection=test_twitter_mongo,
#                                  mongo_address=mongo_address,
#                                  debug=False,
#                                  header=False,
#                                  debug_rows=1000,
#                                  print_progress=1000)
#
# # 1.06 minutes for 20,000 entries using csv reader
# # 34s for 10,000 entries using csv reader
#
# print(get_diagnostics)

print(datetime.now() - start_time)
# address_base_loc = "data/input/address/address_base.csv"
# address_base = AddressBase("data/output/address/", 100000)
# a = address_base.import_address_csv(address_base_loc, terminate_at=-1)
# print(a)