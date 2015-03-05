"""
Description:
Author: Bence Komarniczky
Date:
Python version: 3.4
"""

from ons_twitter.data_import import *
from ons_twitter.data_formats import *
import pymongo

connection = pymongo.MongoClient("192.168.0.82:27017")
mongo_address = connection.twitter.address

original_file = "C:/Users/ONS-BIG-DATA/Documents/TWITTER/twitter/data/input/Tweets_Apr_Oct.csv"
create_test_csv(original_file)

test_file = "C:/Users/ONS-BIG-DATA/Documents/TWITTER/twitter/data/input/Tweets_Apr_Oct_test_subset.csv"

# get_diagnostics = import_one_csv(test_file,
#                                  mongo_address=mongo_address,
#                                  debug=False,
#                                  header=False,
#                                  debug_rows=1000,
#                                  print_progress=True)
# print(get_diagnostics)
# test_file = "data/input/test.csv"
#
# file_ext = find_file_name(test_file)
# print(file_ext)

address_base_loc = "data/input/address/address_base.csv"
address_base = AddressBase("data/output/address/", 100000)
a = address_base.import_address_csv(address_base_loc, terminate_at=-1)
print(a)