"""
Description:    Run clustering algorithm on all valid tweets in the dataset.
                Attach address info and cluster info to all tweets.
Author:         Bence Komarniczky
Date:           16/March/2015
Python version: 3.4
"""

from datetime import datetime

import ons_twitter.cluster as cl


# start counting time
start_time = datetime.now()
print("Starting clustering: ", start_time, "\n")

# specify databases

mongo_address = ("192.168.0.82:27017", "twitter", "address")

twitter_data = ("localhost:30033", "twitter", "tweets")

# start clustering
user_no = cl.cluster_all(twitter_data, mongo_address, chunk_range=range(20, 1000),
                         parallel=False, debug=False)

# give info
print("\n  ****\nFinished clustering at: ", datetime.now(),
      "\n in: ", datetime.now()-start_time,
      "\n Found users:", user_no)