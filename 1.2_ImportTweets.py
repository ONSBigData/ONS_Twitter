"""
Description:    Import Tweet database into mongodb
Author:         Bence Komarniczky
Date:           11/March/2015
Python version: 3.4
"""

from datetime import datetime

import ons_twitter.data_import as di


# set up mongo variables
mongo_address = ("192.168.0.82:27017", "twitter", "address")
twitter_mongo = ("192.168.0.97:30000", "twitter", "tweets")

# point to datafiles
harvested_tweets = "data/input/final_data/Tweets_Apr12_Aug14.csv"
april_tweets = "data/input/final_data/GNIP_April.csv"
aug_oct_tweets = "data/input/final_data/GNIP_August_October.csv"

output_folder = "data/input/new_data/chunks/"

# put them as tuple
files = (harvested_tweets,
         april_tweets,
         aug_oct_tweets)


# slice them up
for file_name in files:
    print("Start slicing: ", file_name, "\n", datetime.now(), "\n")
    di.create_partition_csv(file_name,
                            output_folder=output_folder,
                            num_rows=-1,
                            chunk_size=10000)

# insert tweets
di.import_csv(output_folder,
              mongo_connection=twitter_mongo,
              mongo_address=mongo_address)
