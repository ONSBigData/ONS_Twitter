"""
Description:    Import csv file(s) of Tweets into  mongodb database
                with their closes address point (queried from another mongodb database)
Author:         Bence Komarniczky
Date:           11/March/2015
Python version: 3.4
"""

from datetime import datetime

import ons_twitter.data_import as data_import


"""
Specify mongodb address databases.
There could be more than one to speed up the
import process and import in parallel.
"""

mongo_address = (("192.168.0.82:27017", "twitter", "address"),
                 ("192.168.0.87:28000", "twitter", "address"),
                 ("192.168.0.62:28001", "twitter", "address"),
                 ("192.168.0.97:28002", "twitter", "address"),
                 ("192.168.0.97:28003", "twitter", "address"))

# set the destination mongodb database
twitter_mongo = ("192.168.0.97:30000", "twitter", "tweets")

# point to datafiles
harvested_tweets = "data/input/final_data/Tweets_Apr12_Aug14.csv"
april_tweets = "data/input/final_data/GNIP_April.csv"
aug_oct_tweets = "data/input/final_data/GNIP_August_October.csv"

# folder to put sliced up csv files (this will allow for parallel inserts and address lookups)
output_folder = "data/input/new_data/chunks/"

# convert the file names into a tuple to loop through
files = (harvested_tweets,
         april_tweets,
         aug_oct_tweets)

# slice up each file
for file_name in files:
    print("Start slicing: %s at: %s" % (file_name, datetime.now()))
    data_import.create_partition_csv(file_name,
                                     output_folder=output_folder,
                                     num_rows=-1,
                                     chunk_size=10000)

# insert all files from the output folder. Note that the first argument can be a file as well,
# in which case the function imports that file only
data_import.import_files(output_folder,
                         mongo_connection=twitter_mongo,
                         mongo_address=mongo_address)