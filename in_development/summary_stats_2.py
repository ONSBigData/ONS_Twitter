"""
Description:    Get some more summary statistics from final, cleaned dataset
Author:         Bence Komarniczky
Date:           09/06/2015
Python version: 3.4
"""

import pymongo
import pandas as pd

chunk_id = 0

connection = ("192.168.0.99:30000", "twitter", "tweets")

db = pymongo.MongoClient(connection[0])[connection[1]][connection[2]]

result = db.aggregate([{"$match": {"chunk_id": 1, "cluster.dominant": 1}},
                       {"$group": {"_id": "$cluster.cluster_id", "size": {"$first": "$cluster.count"}}},
                       {"$group": {"_id": "total_dominant", "number_of_dominant_clusters": {"$sum": 1}, "cluster_sizes": {"$push": "$size"}}}
                       ])["result"]


my_list = result[0]["cluster_sizes"]
d = {x: my_list.count(x) for x in my_list}

d = pd.DataFrame.from_dict(d, orient="index")
d.columns = ["count"]
d.sort_index(inplace=True)

print(d)

print(result)