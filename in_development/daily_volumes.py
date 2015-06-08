"""
Description: Get daily volumes from mongodb
Author: Bence Komarniczky
Date: 12/05/2015
Python version: 3.4
"""

from datetime import datetime
import pymongo
import pandas as pd


tweets = pymongo.MongoClient("192.168.0.99:30000")["twitter"]["tweets"]

for chunk_id in range(1000):
    print(chunk_id, datetime.now())
    counts = tweets.aggregate([{"$match": {"chunk_id": chunk_id}},
                               {"$group": {"_id": "$time.date", "count": {"$sum": 1}}}])
    df = pd.DataFrame(list(counts))
    df.set_index(pd.to_datetime("_id"), drop=True, inplace=True)

    if chunk_id == 0:
        all_counts = df
    else:
        all_counts["count"] += df["count"]


all_counts.sort_index().to_csv("daily_volumes.csv", index=True)