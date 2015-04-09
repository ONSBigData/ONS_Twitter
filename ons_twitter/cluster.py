"""
Description:    Code for the DBScan clustering algorithm.
Author:         Bence Komarniczky
Date:           09/03/2015
Python version: 3.4
"""

from datetime import datetime
import time

import numpy as np
from bson.son import SON
from pymongo.errors import OperationFailure, ConnectionFailure, AutoReconnect
import pymongo
from joblib import Parallel, delayed

from ons_twitter.supporting_functions import distance as simple_distance


def create_dictionary_for_chunk(mongo_connection, chunk_id):
    """
    Takes a mongo_db connection and a chunk_id as from the twitted mongodb database. Returns a dictionary,
    where each key is a user_id and each value is the list of all tweets from that user.

    :param mongo_connection:    List of mongodb connection parameters to twitter database. [ip, database, collection]
    :param chunk_id:        Number of chunk to process. 0-999
    :return:                Dictionary with user_id: [tweets]
    """

    # initiate dictionary
    tweets_by_user = {}

    try:
        # set up query
        mongo_client = pymongo.MongoClient(mongo_connection[0], w=0)[mongo_connection[1]][mongo_connection[2]]
        query = {"chunk_id": chunk_id}

        # collect cursor
        cursor = mongo_client.find(query, {"_id": 1, "user_id": 1, "tweet.coordinates": 1})

        # initiate dictionary
        tweets_by_user = {}
        for new_tweet_mongo in cursor:
            new_tweet = [new_tweet_mongo["_id"],
                         new_tweet_mongo["user_id"],
                         new_tweet_mongo["tweet"]["coordinates"]]

            # insert into dictionary
            try:
                tweets_by_user[new_tweet_mongo["user_id"]].append(new_tweet)
            except KeyError:
                tweets_by_user[new_tweet_mongo["user_id"]] = [new_tweet]
        print("Chunk collected: ", chunk_id, datetime.now())
    except OperationFailure:
        for x in range(5):
            print("Retry connection to: ", mongo_connection)
            time.sleep(5)
            try:
                # set up query
                mongo_client = pymongo.MongoClient(mongo_connection[0], w=0)[mongo_connection[1]][mongo_connection[2]]
                query = {"chunk_id": chunk_id}

                # collect cursor
                cursor = mongo_client.find(query, {"_id": 1, "user_id": 1, "tweet.coordinates": 1})

                # initiate dictionary
                tweets_by_user = {}
                for new_tweet_mongo in cursor:
                    new_tweet = [new_tweet_mongo["_id"],
                                 new_tweet_mongo["user_id"],
                                 new_tweet_mongo["tweet"]["coordinates"]]

                    # insert into dictionary
                    try:
                        tweets_by_user[new_tweet_mongo["user_id"]].append(new_tweet)
                    except KeyError:
                        tweets_by_user[new_tweet_mongo["user_id"]] = [new_tweet]
                print("Chunk collected: ", chunk_id, datetime.now())
                break
            except OperationFailure:
                continue

    return tweets_by_user


def _euclidean_distances_matrix(vector1, vector2):
    """
    Takes two complex vectors and returns a euclidean distance matrix.
    Input should be A[j] = x[j] + i*y[j]

    :param vector1: Complex numpy vector
    :param vector2: Complex numpy vector
    :return:        Euclidean distance matrix
    """

    # get two matrices with all possible point combinations
    m, n = np.meshgrid(vector1, vector2)

    # take the difference of their absolute values, this is the euclidean distance by definition
    distance_array = abs(m - n)

    # convert them into integers
    distance_array_integer = distance_array.astype('int32')

    return distance_array_integer


def distance_matrix(point_list, block_size=1000):
    """
    For a given list of input points (Tweets) returns the distance matrix. Uses numpy arrays and complex numbers.
    In order to fit into memory, there is a block_size parameter that breaks up the computation into chunks.
    With block_size= 1000, it can process 21,000 tweets within 22 seconds, using approximately 7Gb ram.

    :param point_list: _id, user_id, coordinates tuples
    :param block_size: break point for chunks, tweets over this size will be processed in chunks.
    :return:    numpy array of distance matrix
    """

    # create numpy array from input points
    all_points = np.array([complex(one_tweet[2][0], one_tweet[2][1]) for one_tweet in point_list])

    # count the size of input
    n = len(point_list)

    # do blocks/don't do blocks
    if n < block_size:

        # simply calculate distance
        distance_array_integer = _euclidean_distances_matrix(all_points, all_points)

    else:
        # initiate empty matrix
        distance_array_integer = np.empty((n, n), dtype="int32")

        # calculate the number of required blocks
        blocks = n // block_size

        # iterate over block ids
        for row_id in range(blocks + 1):
            # attach new strip to matrix
            distance_array_integer[:, (row_id * block_size):((row_id + 1) * block_size)] = \
                _euclidean_distances_matrix(all_points[(row_id * block_size):((row_id + 1) * block_size)],
                                            all_points)

    return distance_array_integer


def create_one_cluster(cluster_points, remaining_mask, distance_array, eps=20, graphical_debug=False):
    """
    Create one new cluster for the user and return the remaining points.

    :param cluster_points:  list of cluster points, as supplied to the distance_matrix function
    :param remaining_mask:  list of a list and a numpy array. First list indicates the available row positions,
                            that have not been searched before. An updated version of this will be returned.
    :param distance_array:  numpy integer array of approximated euclidean distances, output of distance matrix
                            function.
    :param eps:             distance parameter for dbscan algorithm
    :return: new_cluster:   list of all points from cluster_points, belonging to the new cluster
    :return: remaining_mask:updated remaining mask, None if user's tweets have been exhausted
    """

    # check if any mask remains
    if len(remaining_mask[1]) == 0:
        return None, remaining_mask

    # pick first element, start populating new_cluster
    search_row = remaining_mask[0].pop(0)
    new_cluster = [cluster_points[search_row]]

    # delete first column
    remaining_mask[1] = np.delete(remaining_mask[1], 0)

    # search for that one row
    found = remaining_mask[1][distance_array[search_row, remaining_mask[1]] < eps]

    # find all other rows that are close to row 1
    search_these = []
    for found_index in found:
        # add point to cluster and remove column from distance array
        new_cluster.append(cluster_points[found_index])
        remaining_mask[1] = np.delete(remaining_mask[1], np.where(remaining_mask[1] == found_index))
        search_these.append(found_index)

    # duplicate search list for looping
    new_search_list = search_these[:]

    # do this until cluster cannot grow any more
    while len(new_search_list) > 0:
        # loop through all found indices
        for search_row in search_these:

            # remove searched row from mask
            remaining_mask[0].remove(search_row)

            # search row=
            found = remaining_mask[1][distance_array[search_row, remaining_mask[1]] < eps]

            # search all found indices
            for found_index in found:
                # add point to cluster and remove column from distance array
                new_cluster.append(cluster_points[found_index])
                remaining_mask[1] = np.delete(remaining_mask[1], np.where(remaining_mask[1] == found_index))
                search_these.append(found_index)

            # remove search row if hasn't been removed yet
            try:
                new_search_list.remove(search_row)
            except ValueError:
                # skip if value has already been deleted
                continue

        # update search_these list for while statement
        search_these = new_search_list[:]

    if graphical_debug:
        import matplotlib.pyplot as plt

        x = [one_point[2][0] for one_point in new_cluster]
        y = [one_point[2][1] for one_point in new_cluster]

        plt.plot(x, y, 'ro')
        plt.show()

    return new_cluster, remaining_mask


def create_cluster_info(complete_cluster, cluster_name, mongo_address_list, min_points=3):
    """
    Returns more information for the cluster. Mean of distances, maximum distance, standard deviation of distances
    from cluster centroid.

    :param complete_cluster:        list of all points in completed cluster
    :param cluster_name:            name to include in cluster_id
    :param mongo_address_list:           pymongo connection to geo_indexed address base [ip, database, collection]
    :param min_points:              number of points in cluster for cluster classification
    :return:                        json formatted dictionary for mongodb twitter["cluster"] insert
    """
    # convert cluster name to string in case of numeric input
    cluster_name = str(cluster_name)

    # get coordinates and mean centroid coordinates
    coordinate_points = np.array([one_tweet[2] for one_tweet in complete_cluster])
    cluster_centroid = np.array([one_tweet[2] for one_tweet in complete_cluster]).mean(0)

    # convert to complex numbers for easier calculations
    complex_coordinates = 1j * coordinate_points[..., 1] + coordinate_points[..., 0]
    complex_centroid = 1j * cluster_centroid[..., 1] + cluster_centroid[..., 0]

    # calculate all the distances
    distances = _euclidean_distances_matrix(complex_coordinates, complex_centroid)

    # start dictionary
    cluster_info = {
        "count": len(complete_cluster),
        "centroid_coordinates": [int(cluster_centroid[0]),
                                 int(cluster_centroid[1])],
        "stats": {
            "max_distance": float('%.3f' % round(distances.max(), 3)),
            "mean_distance": float('%.3f' % round(distances.mean(), 3)),
            "standard_deviation_distance": float('%.3f' % round(distances.std(), 3))
        }
    }

    # get cluster_type
    if min_points <= cluster_info["count"]:
        cluster_info["type"] = "cluster"
    else:
        cluster_info["type"] = "noise"

    # initiate empty variables
    place = "MISSING"
    cluster_info["address"] = "NA"

    # find closest address
    if cluster_info["type"] == "cluster":
        # create pymongo connection
        try:
            mongo_address = pymongo.MongoClient(host=mongo_address_list[0], w=0)[mongo_address_list[1]][
                mongo_address_list[2]]
        except ConnectionFailure:
            for x in range(5):
                try:
                    print("server is busy", mongo_address_list, " retry: ", x)
                    time.sleep(1)
                    mongo_address = pymongo.MongoClient(host=mongo_address_list[0], w=0)[mongo_address_list[1]][
                        mongo_address_list[2]]
                    break

                except ConnectionFailure:
                    pass

        query = {"coordinates": SON([("$near", (float(cluster_centroid[0]), float(cluster_centroid[1]))),
                                     ("$maxDistance", 300)])}
        try:
            closest_address_list = mongo_address.find(query, {"_id": 0}).limit(1)[0]
            cluster_info["address"] = closest_address_list
            cluster_info["address"]["distance"] = float('%.3f' %
                                                        round(simple_distance(closest_address_list["coordinates"],
                                                                              cluster_centroid), 3))

            place = closest_address_list["postcode"].replace(" ", "_")
        except IndexError:
            # no address has been found within 300m
            cluster_info["address"] = "NA"
            place = "NA"
        except OperationFailure:
            print("No address base available!")
            cluster_info["address"] = "NA"
            place = "FAILURE"
        except AutoReconnect:
            print("Address base is busy!")
            for x in range(5):
                try:
                    time.sleep(1)
                    closest_address_list = mongo_address.find(query, {"_id": 0}).limit(1)[0]
                    cluster_info["address"] = closest_address_list
                    cluster_info["address"]["distance"] = float('%.3f' %
                                                                round(
                                                                    simple_distance(closest_address_list["coordinates"],
                                                                                    cluster_centroid), 3))

                    place = closest_address_list["postcode"].replace(" ", "_")
                    break

                except IndexError:
                    # no address has been found within 300m
                    cluster_info["address"] = "NA"
                    place = "NA"
                    break

                except OperationFailure:
                    print("No address base available!")
                    cluster_info["address"] = "NA"
                    place = "FAILURE"
                    break

                except AutoReconnect:
                    print("Try failed:", x)
                    continue
    else:
        cluster_info["address"] = "NA_noise"
        place = "noise"

    cluster_info["cluster_id"] = "%s_%s_%s" % (complete_cluster[0][1], place, cluster_name)

    return cluster_info, list(distances[0])


def cluster_one_user(user_id, tweets_by_user, mongo_address, eps=20, min_points=3,
                     debug=False, graph_debug=False):
    """
    Cluster all the tweets of one user from a twitter dictionary.

    :param user_id:         integer for twitter user_id
    :param tweets_by_user:  a dictionary of user/list of tweets pairs. Output of create_dictionary_for_chunk
    :param destination:     "json" or a mongodb connection if tweets to be updated there
    :param mongo_address:   pymongo connection to an address base
    :param eps:             distance parameter for mongodb
    :param min_points:      minimum number of points in a valid cluster
    :return:                updates mongodb database/json document for user, with cluster info
    """
    debug_threshold = 300

    # grab all tweets of specific user
    all_tweets = tweets_by_user[user_id]

    if debug and len(all_tweets) > debug_threshold:
        print(user_id, len(all_tweets), datetime.now())

    # create distance matrix
    p1_time = datetime.now()
    distance_array = distance_matrix(all_tweets)
    if debug and len(all_tweets) > debug_threshold:
        print(" ** matrix done in ", datetime.now() - p1_time)

    # create mask and switch
    index = 0
    continue_clustering = True
    mask = [[x for x in range(len(all_tweets))],
            np.arange(len(all_tweets), dtype=np.uint32)]

    p2_time = datetime.now()

    # mongo_info holder
    mongo_updates = []

    # do clustering till set is exhausted
    while continue_clustering:

        new_cluster, mask = create_one_cluster(all_tweets, mask, distance_array, eps=eps, graphical_debug=graph_debug)

        # get info and update database if new info is found
        if new_cluster is not None:
            # grab new info

            new_info, distances = create_cluster_info(new_cluster, index, mongo_address, min_points=min_points)

            # find tweet ids to update
            tweet_ids_to_update = [tweet[0] for tweet in new_cluster]

            one_update_rule = [(tweet_ids_to_update[i], new_info, distances[i], len(all_tweets)) for i in
                               range(len(tweet_ids_to_update))]
            mongo_updates.append(one_update_rule)

            index += 1

        else:
            # terminate clustering if user tweets have been used up
            continue_clustering = False

    if debug and len(all_tweets) > debug_threshold:
        print(" ** clustering done: ", len(all_tweets), datetime.now() - p2_time)

    # for cluster in mongo_updates:
    # for tweet_info in cluster:
    # all_updates.find({"_id": tweet_info[0]}).update_one({"$set": {"cluster": tweet_info[1],
    # "total_tweets_for_user": len(all_tweets),
    # "tweet.distance_from_centroid": float(
    #                                                                           tweet_info[2])}})

    return mongo_updates


def cluster_one_chunk(mongo_connection, mongo_address, chunk_id, debug=False, debug_user=-1,
                      graph_debug=False, return_csv=False):
    """
    Cluster all the tweets for one chunk.

    :param mongo_connection:    List of mongo parameters to database of tweets. [ip, database, collection]
    :param mongo_address:       List of mongo parameters to address_base. [ip, database, collection]
    :param chunk_id:            Chunk number.
    :return:                    Number of users clustered.
    """

    if chunk_id <= 8:
        print("core", chunk_id, "going to sleep for a bit...")
        time.sleep(30 * chunk_id)
        print("core", chunk_id, "finished sleep")
    start_time = datetime.now()

    # grab the data
    tweets_by_user_dict = create_dictionary_for_chunk(mongo_connection, chunk_id=chunk_id)

    if debug_user >= 0:
        tweets_by_user_dict = {debug_user: tweets_by_user_dict[debug_user]}

    # # establish mongo connection
    # destination = pymongo.MongoClient(mongo_connection[0], w=1)[mongo_connection[1]][mongo_connection[2]]
    # bulk_updates = destination.initialize_unordered_bulk_op()

    mongo_updates = []
    # cluster each user
    for user_id in tweets_by_user_dict.keys():
        new_updates = cluster_one_user(user_id=user_id,
                                       tweets_by_user=tweets_by_user_dict,
                                       mongo_address=mongo_address,
                                       debug=debug,
                                       graph_debug=graph_debug)
        mongo_updates.append(new_updates)

    print("***Starting updates ", chunk_id, "at: ", datetime.now(), mongo_connection)
    p6_time = datetime.now()
    # bulk_updates.execute()
    destination = pymongo.MongoClient(mongo_connection[0], w=0)[mongo_connection[1]][mongo_connection[2]]
    bulk_updates = destination.initialize_ordered_bulk_op()

    for user in mongo_updates:
        for cluster in user:
            for tweet_info in cluster:
                bulk_updates.find({"_id": tweet_info[0]}).update({"$set": {"cluster": tweet_info[1],
                                                                           "total_tweets_for_user": int(tweet_info[3]),
                                                                           "tweet.distance_from_centroid": float(
                                                                               tweet_info[2])}})
    bulk_updates.execute()

    if debug:
        print("\nUpdates took: ", datetime.now() - p6_time)

    print("  *******Finished ", chunk_id, "at: ", datetime.now(), "   in ", datetime.now() - start_time,
          "  updates took: ", datetime.now() - p6_time)

    if return_csv:
        return mongo_updates

    return len(tweets_by_user_dict)


def cluster_all(mongo_connection, mongo_address, chunk_range=range(1000),
                parallel=True, debug=False, num_cores=-1):
    """
    Cluster all tweets found in collection.
    :param mongo_connection:    List of mongo parameters to database of tweets. [ip, database, collection]
    :param mongo_address:       List of mongo parameters to address_base. [ip, database, collection]
    :param chunk_range:         Optional range for chunk ids to cluster.
    :return:                    Number of users clustered.
    """

    # decide on parallel mongodb lookup

    if parallel:
        if type(mongo_address[0]) is str:
            all_users = Parallel(n_jobs=num_cores)(delayed(cluster_one_chunk)(mongo_connection,
                                                                              mongo_address,
                                                                              index_num,
                                                                              debug)
                                                   for index_num in chunk_range)
        else:
            # verbose
            print("\nMore than one address base were supplied!",
                  "\nUsing all of them:\n")
            for one_address in mongo_address:
                print(one_address)

            print("*****\n")

            # create an iterable
            dummy_mongo = mongo_address * ((len(chunk_range) // len(mongo_address)) + 1)
            dummy_mongo = dummy_mongo[:len(chunk_range)]
            mongo_chunk_iter = []

            dummy_mongo_servers = mongo_connection * ((len(chunk_range) // len(mongo_connection)) + 1)
            dummy_mongo_servers = dummy_mongo_servers[:len(chunk_range)]

            i = 0
            for address_param in dummy_mongo:
                mongo_chunk_iter.append((dummy_mongo_servers[i], address_param, chunk_range[i]))
                i += 1

            # call parallel
            all_users = Parallel(n_jobs=num_cores)(delayed(cluster_one_chunk)(param_collection[0],
                                                                              param_collection[1],
                                                                              param_collection[2],
                                                                              debug)
                                                   for param_collection in mongo_chunk_iter)
    else:
        print("doing it in serial")
        all_users = 0

        if type(mongo_address[0]) is not str:
            mongo_address = mongo_address[0]
            print("Using only 1st address base: ", mongo_address)

        if type(mongo_connection[0]) is not str:
            mongo_connection = mongo_connection[0]
            print("Using only 1st mongo_server: ", mongo_connection)

        for index_num in chunk_range:
            all_users += cluster_one_chunk(mongo_connection,
                                           mongo_address,
                                           index_num,
                                           debug)

            all_users = [0, all_users]

    return sum(all_users)
