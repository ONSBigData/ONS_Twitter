"""
Description:    Get some more summary statistics from final, cleaned dataset
Author:         Bence Komarniczky
Date:           09/06/2015
Python version: 3.4
"""

import pymongo
import pandas as pd


def get_stats_one_chunk(chunk_id, connection=("192.168.0.99:30000", "twitter", "tweets")):
    db = pymongo.MongoClient(connection[0])[connection[1]][connection[2]]

    # count users with valid residential clusters and keep track of their size distribution
    dominant_users = db.aggregate([{"$match": {"chunk_id": chunk_id, "cluster.dominant": 1}},
                                   {"$group": {"_id": "$cluster.cluster_id", "size": {"$first": "$cluster.count"}}},
                                   {"$group": {"_id": "total_dominant",
                                               "number_of_dominant_clusters": {"$sum": 1},
                                               "cluster_sizes": {"$push": "$size"}}}
                                   ])["result"]

    count_dist = pd.DataFrame.from_dict({x: dominant_users[0]["cluster_sizes"].count(x)
                                         for x in dominant_users[0]["cluster_sizes"]},
                                        orient="index")
    count_dist.columns = ["count"]
    count_dist.sort_index(inplace=True)

    number_of_dominant_users = dominant_users[0]["number_of_dominant_clusters"]

    # grab the number of users who did not have any valid clusters
    no_cluster_users = db.aggregate([{"$match": {"chunk_id": chunk_id}},
                                     {"$group": {"_id": "$cluster.cluster_id",
                                                 "user_id": {"$first": "$user_id"},
                                                 "cluster_type": {"$first": "$cluster.type"}}},
                                     {"$group": {"_id": "$user_id",
                                                 "user_cluster_types": {"$addToSet": "$cluster_type"}}}
                                     ])["result"]

    users_by_types = {"only_noise": 0, "only_cluster": 0, "both": 0}

    for one_user in no_cluster_users:
        if len(one_user["user_cluster_types"]) == 2:
            users_by_types["both"] += 1
        elif one_user["user_cluster_types"][0] == "noise":
            users_by_types["only_noise"] += 1
        elif one_user["user_cluster_types"][0] == "cluster":
            users_by_types["only_cluster"] += 1
        else:
            print("unexpected input type!", one_user)

    user_types = pd.DataFrame(users_by_types, index=[chunk_id])


    # grab users by residential and commercial clusters
    res_com_users = db.aggregate([{"$match": {"chunk_id": chunk_id, "cluster.type": "cluster"}},
                                  {"$group": {"_id": "$cluster.cluster_id",
                                              "user_id": {"$first": "$user_id"},
                                              "type": {"$first": "$cluster.address.classification.abbreviated"}}},
                                  {"$match": {"type": {"$in": ["C", "R"]}}},
                                  {"$group": {"_id": "$user_id", "all_types": {"$addToSet": "$type"}}}
                                  ])["result"]

    users_with_both_res_com = 0

    for user in res_com_users:
        if len(user["all_types"]) == 2:
            users_with_both_res_com += 1

    user_types["users_with_RC"] = [users_with_both_res_com]
    user_types["users_with_dominant_clusters"] = [number_of_dominant_users]


    # users with valid residential clusters for each month
    all_months = db.aggregate([{"$match": {"chunk_id": chunk_id,
                                           "cluster.type": "cluster",
                                           "cluster.address.classification.abbreviated": "R"}},
                               {"$group": {"_id": {"month": "$time.month",
                                                   "cluster_id": "$cluster.cluster_id"},
                                           "user_id": {"$first": "$user_id"},
                                           "count": {"$sum": 1}}},
                               {"$match": {"count": {"$gt": 2}}},
                               {"$group": {"_id": "$user_id", "months": {"$addToSet": "$_id.month"}}},
                               {"$project": {"months": 1, "_id": 0}}
                               ])["result"]


    number_months = {}
    for int_months in [len(x["months"]) for x in all_months]:
        try:
            number_months[int_months] += 1
        except KeyError:
            number_months[int_months] = 1

    number_months = pd.DataFrame.from_dict(number_months, orient="index")
    number_months.columns = ["Users with months"]

    return_dict = {"user_types": user_types,
                   "dominant_distribution": count_dist,
                   "months_distribution": number_months}

    print(return_dict["user_types"])
    print(return_dict["dominant_distribution"])
    print(return_dict["months_distribution"])

    return return_dict


results = []
for chunk_number in range(3):
    results.append(get_stats_one_chunk(chunk_number))


# concatenate data frames together
dominant_distribution = results[0]["dominant_distribution"]
months_distribution = results[0]["months_distribution"]

for chunk_number in range(1, len(results)):
    dominant_distribution = dominant_distribution.add(results[chunk_number]["dominant_distribution"], fill_value=0)
    months_distribution = months_distribution.add(results[chunk_number]["months_distribution"], fill_value=0)

user_types = pd.concat([one_item["user_types"] for one_item in results])

# print/save results
print(user_types)
print(dominant_distribution)
print(months_distribution)