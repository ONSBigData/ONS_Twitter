"""
Description:
Author: Bence Komarniczky
Date:
Python version: 3.4
"""

from datetime import datetime
import ons_twitter.cluster as cl

start_time = datetime.now()

address_mongo = (("127.0.0.1:27017", "twitter", "address"),
                 ("192.168.0.82:27017", "twitter", "address"),
                 ("192.168.0.62:28001", "twitter", "address"))

test_twitter_mongo = ("127.0.0.1:27017", "test", "tweets")

if __name__ == "__main__":
    cl.cluster_all(mongo_connection=test_twitter_mongo,
                   mongo_address=address_mongo)

print(datetime.now() - start_time)