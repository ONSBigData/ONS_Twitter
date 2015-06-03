"""
Description:
    Using the dominant flag collapse the database and extract people's language information
    by geography.
Author: Bence Komarniczky
Date:   03/06/2015
Python version: 3.4
"""

import pymongo
import pandas as pd
from datetime import datetime


# establish pymongo connection
db = pymongo.MongoClient("192.168.0.99:30000")["twitter"]["tweets"]

all_counts = pd.DataFrame()

for chunk_id in range(1000):
    print("chunk: %3.d  time: %s" % (chunk_id, str(datetime.now())))

    # aggregate one chunk
    df = db.aggregate([{"$match": {"chunk_id": chunk_id, "cluster.dominant": 1}},
                       {"$group": {"_id": "$cluster.cluster_id",
                                   "user_id": {"$first": "$user_id"},
                                   "oslaua": {"$first": "$cluster.address.levels.oslaua"},
                                   "languages": {"$addToSet": "$tweet.language"}}}])

    df = list(df)

    for a in df:
        a["languages"].sort()
        a["languages"] = "_".join(a["languages"])

    dataset = pd.DataFrame(df)

    grouped = dataset.groupby(by=["oslaua", "languages"])
    agg_df = grouped.count()

    if chunk_id == 0:
        all_counts = agg_df
    else:
        all_counts = all_counts.add(agg_df, fill_value=0, axis=0)


all_counts.to_csv("languages.csv")