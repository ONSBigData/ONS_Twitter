"""
Description:
    Look at users' languages in the Twitter data by geography.
Author: Bence Komarniczky
Date: 02/06/2015
Python version: 3.4
"""

from datetime import datetime
import pymongo
from joblib import Parallel,delayed


# establish mongo connection
db = pymongo.MongoClient("192.168.0.99:30000")["twitter"]["tweets"]


def find_and_update_dominant_clusters(chunk_id):
    print("Starting chunk id %3.d at %s" % (chunk_id, str(datetime.now())))
    # get the count for each dominant cluster
    by_cluster = db.aggregate([{"$match": {"chunk_id": chunk_id, "cluster.type": "cluster",
                                           "cluster.address": {"$ne": "NA"},
                                           "cluster.address.classification.abbreviated": "R"}},
                               {"$group": {"_id": "$user_id", "cluster_count": {"$max": "$cluster.count"}
                                           }}])

    by_cluster_list = list(by_cluster)

    bulk = db.initialize_unordered_bulk_op()

    # search for these dominant clusters to retrieve whole documents
    for dominant_cluster in by_cluster_list:
        bulk.find({"chunk_id": chunk_id,
                   "user_id": dominant_cluster["_id"],
                   "cluster.address.classification.abbreviated": "R",
                   "cluster.count": dominant_cluster["cluster_count"]}
                  ).update({"$set": {"cluster.dominant": 1}})

    print("Starting inserts for chunk: %3.d at %s number of dominant clusters: %4.d" % (chunk_id,
                                                                                        str(datetime.now()),
                                                                                        len(by_cluster_list)))
    # execute command
    bulk.execute()
    print("Finished inserts for chunk: %3.d at %s" % (chunk_id, str(datetime.now())))


# do updates in parallel
Parallel(n_jobs=-1)(delayed(find_and_update_dominant_clusters)(chunk_id) for chunk_id in range(1000))
