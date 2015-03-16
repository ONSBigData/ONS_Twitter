"""
Description:    TEST Import Tweet database into mongodb
Author:         Bence Komarniczky
Date:           11/March/2015
Python version: 3.4
"""

import ons_twitter.data_import as di


# set up mongo variables
mongo_address = ("192.168.0.82:27017", "twitter", "address")
twitter_mongo = ("192.168.0.97:30000", "twitter", "tweets")

april_tweets = "data/input/final_data/GNIP_April.csv"

output_folder = "data/input/new_data/chunks_test/"

di.create_partition_csv(april_tweets,
                        output_folder=output_folder,
                        num_rows=1000,
                        chunk_size=100)

# insert tweets
di.import_csv(output_folder,
              mongo_connection=twitter_mongo,
              mongo_address=mongo_address)
