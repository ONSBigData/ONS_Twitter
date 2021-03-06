"""
Description: test this2
Author: Bence Komarniczky
Date:
Python version: 3.4
"""

from datetime import datetime

import ons_twitter.cluster as cl


start_time = datetime.now()


mongo_address = (("127.0.0.1:27017", "twitter", "address"),
                 ("192.168.0.82:27017", "twitter", "address"),
                 ("192.168.0.62:28001", "twitter", "address"),
                 ("192.168.0.87:28000", "twitter", "address"),
                 ("192.168.0.97:28002", "twitter", "address"))
#
# mongo_address = ("192.168.0.82:27017", "twitter", "address")

test_twitter_mongo = ("127.0.0.1:27017", "test", "tweets")

if __name__ == "__main__":
    cl.cluster_all(mongo_connection=test_twitter_mongo,
                   mongo_address=mongo_address)

print(datetime.now() - start_time)