"""
Description:
Author: Bence Komarniczky
Date:
Python version: 3.4
"""

from ons_twitter.data_import import *
import pymongo

connection = pymongo.MongoClient("192.168.0.82:27017")
mongo_address = connection.twitter.address

test_file = "C:/Users/ONS-BIG-DATA/Documents/TWITTER/twitter/data/input/Tweets_Apr_Oct_test_subset.csv"

get_diagnostics = import_one_csv(test_file,
                                 mongo_address=mongo_address,
                                 debug=True,
                                 header=False,
                                 debug_rows=100)
print(get_diagnostics)