"""
Description:    Get the distribution of cluster sizes accross all clusters.
Author:         Bence Komarniczky
Date:           29/06/2015
Python version: 3.4
"""

from datetime import datetime

import pymongo
import pandas as pd
from joblib import Parallel, delayed

# set the number of chunks to process / for debugging
CHUNKS_TO_PROCESS = 1000


def get_dist_one_chunk(chunk_id, connection=("192.168.0.99:30000", "twitter", "tweets")):
    start_time = datetime.now()
    print("Doing chunk_id: %d, at: %s" % (chunk_id, start_time))

    db = pymongo.MongoClient(connection[0])[connection[1]][connection[2]]

    cluster_dist = db.aggregate([{"$match": {"chunk_id": chunk_id, "cluster.type": "cluster"}},
                                 {"$group": {"_id": "$cluster.cluster_id", "size": {"$first": "$cluster.count"}}},
                                 {"$group": {"_id": "total_dominant",
                                             "cluster_sizes": {"$push": "$size"}}}
                                 ])


    cluster_dist = list(cluster_dist)

    count_dist = pd.DataFrame.from_dict({x: cluster_dist[0]["cluster_sizes"].count(x)
                                         for x in cluster_dist[0]["cluster_sizes"]},
                                        orient="index")

    count_dist.columns = ["count"]
    count_dist.sort_index(inplace=True)

    print("Finished querying chunk: %3.d at: %s in %s" % (chunk_id, datetime.now(), datetime.now() - start_time))

    return count_dist


results = Parallel(n_jobs=-1)(delayed(get_dist_one_chunk)(chunk_number) for chunk_number in range(CHUNKS_TO_PROCESS))


clust_dist = results[0]
for chunk_number in range(1, len(results)):
    clust_dist = clust_dist.add(results[chunk_number], fill_value=0)


clust_dist.to_csv("cluster_distribution.csv")