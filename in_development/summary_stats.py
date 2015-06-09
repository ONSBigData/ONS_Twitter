"""
Description:    Derive summary statistics from final clustered twitter database.
Author:         Bence Komarniczky
Date:           09/06/2015
Python version: 3.4 (runs on linux only!)
"""

from datetime import datetime
import pymongo
import pandas as pd
from joblib import Parallel, delayed


def get_counts(chunk_id, database=("192.168.0.99:30000", "twitter", "tweets")):
    """
    Grab aggregate cluster address statistics from the specified database per chunk.
    :param chunk_id: (int) chunk_id index in database
    :param database: (3-tuple) mongodb database parameters
    :return: pandas data frame with 2 columns, _id: address class and
             count_by_group: number of clusters with such address
    """

    print("Starting chunk_id %3.d, at: %s" % (chunk_id, datetime.now()))

    # establish mongo connection
    db = pymongo.MongoClient(database[0])[database[1]][database[2]]

    # run aggregation query
    result = db.aggregate([{"$match": {"chunk_id": chunk_id, "cluster.type": "cluster"}},
                           {"$group": {"_id": "$cluster.cluster_id",
                                       "address_type": {"$first": "$cluster.address.classification.abbreviated"}}},
                           {"$group": {"_id": "$address_type", "count_by_group": {"$sum": 1}}}])

    # create data frame and rename None column index for clusters with no addresses
    df = pd.DataFrame(list(result))
    try:
        col_index = df.columns.get_loc("_id")
        row_index = pd.isnull(df["_id"]).tolist().index(True)
        df.iloc[row_index, col_index] = "NoAddress"
    except ValueError:
        pass

    # set index
    df.set_index("_id", inplace=True)

    return df

# run query in parallel
results = Parallel(n_jobs=-1)(delayed(get_counts)(chunk_id) for chunk_id in range(1000))

# add up data frames
new_df = results[0]
for index_num in range(1, len(results)):
    new_df = new_df.add(results[index_num], fill_value=0)

# write final df to csv
print("Finished process at %s" % datetime.now())
new_df.to_csv("summary.csv")