"""
Description:    Derive summary statistics from final clustered twitter database.
Author:         Bence Komarniczky
Date:           09/06/2015
Python version: 3.4
"""

from datetime import datetime

import pymongo
import pandas as pd
from joblib import Parallel, delayed


def get_counts(chunk_id, database=("192.168.0.99:30000", "twitter", "tweets")):
    print("Starting chunk_id %3.d, at: %s" % (chunk_id, datetime.now()))
    db = pymongo.MongoClient(database[0])[database[1]][database[2]]

    result = db.aggregate([{"$match": {"chunk_id": chunk_id, "cluster.type": "cluster"}},
                           {"$group": {"_id": "$cluster.cluster_id",
                                       "address_type": {"$first": "$cluster.address.classification.abbreviated"}}},
                           {"$group": {"_id": "$address_type", "count_by_group": {"$sum": 1}}}])

    df = pd.DataFrame(list(result))
    try:
        col_index = df.columns.get_loc("_id")
        row_index = pd.isnull(df["_id"]).tolist().index(True)
        df.iloc[row_index, col_index] = "NoAddress"
    except ValueError:
        pass

    df.set_index("_id", inplace=True)

    return df


results = Parallel(n_jobs=-1)(delayed(get_counts)(chunk_id) for chunk_id in range(1000))


new_df = results[0]
for index_num in range(1, len(results)):
    new_df = new_df.add(results[index_num], fill_value=0)

print("Finished process at %s" % datetime.now())
new_df.to_csv("summary.csv")


