"""
Description:
Author: Bence Komarniczky
Date:
Python version: 3.4
"""
import pymongo
from ons_twitter.data_formats import *

test_twitter_mongo = pymongo.MongoClient("127.0.0.1:27017").test.tweets

