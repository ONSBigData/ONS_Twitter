"""
Description:    Take out all the robots from the database that have been found so far and move them
                to a new collection called robots.
Author:         Bence Komarniczky
Date:           08/06/2015
Python version: 3.4
"""

import csv
from datetime import datetime

import numpy as np
import pymongo
from joblib import delayed, Parallel


# point to files, with user_ids for robots
recent_robots = "special_csv/robots.csv"
manual_robots = "special_csv/manual_robots.csv"
found_robots = "special_csv/found_robots_1000.csv"

# initialise empty set
robot_list = set([])


def read_one_file(file_path, user_col):
    """
    Read one csv file, containing robot user_ids and add to the
    set of global robots.
    :param file_path:   Pointer to file.
    :param user_col:    Pythonian index of csv column containing user_ids.
    :return:            Nothing. Will append to robot list
    """
    global robot_list

    # open and read csv file
    with open(file_path, 'r') as infile:
        input_csv = csv.reader(infile)
        for row in input_csv:
            robot_list.add(int(row[user_col]))

# read all 3 files in sequence
read_one_file(recent_robots, 0)
read_one_file(manual_robots, 0)
read_one_file(found_robots, 1)

# number of robots should be 242 at this point
assert len(robot_list) == 242

# sort the set into a list
robot_list = sorted(robot_list, key=lambda one_id: one_id % 1000)

# specify databases
db = ("192.168.0.99:30000", "twitter", "tweets")
db_robots = ("192.168.0.99:30000", "twitter", "robots")


def move_one_user(user_id, from_data_base=db, to_data_base=db_robots):
    """
    Move one user to the new robots database.
    :param user_id:         user id of robot user
    :param from_data_base:  source database
    :param to_data_base:    destination database
    :return:                number of tweets moved

    :type user_id           int
    :type from_data_base    tuple[str] | list[str]
    :type to_data_base      tuple[str] | list[str]
    :rtype                  int
    """

    # assert input types
    assert type(user_id) is int, "User_id must be an integer!"
    assert type(from_data_base) is tuple and len(from_data_base) == 3, "from_data_base must be a tuple of form" \
                                                                       "(ip:host, database, collection)"
    assert type(to_data_base) is tuple and len(to_data_base) == 3, "to_data_base must be a tuple of form" \
                                                                   "(ip:host, database, collection)"
    assert from_data_base != to_data_base, "Source and destination database must be distinct"

    # show user_id to be moved
    start_time = datetime.now()
    print("%20.d  time: %s" % (user_id, start_time))

    mongo_from = pymongo.MongoClient(from_data_base[0])[from_data_base[1]][from_data_base[2]]
    mongo_to = pymongo.MongoClient(to_data_base[0])[to_data_base[1]][to_data_base[2]]

    # query database and set up bulk insert
    cursor = mongo_from.find({"chunk_id": user_id % 1000, "user_id": user_id})
    cursor_list = list(cursor)

    # if cursor is empty skip user
    if len(cursor_list) == 0:
        print("No tweets for user %20.d, skipping user!" % user_id)
        return 0

    bulk = mongo_to.initialize_unordered_bulk_op()

    # initialise list to record unique _id values
    unique_ids = []

    # iterate over cursor
    counter = 0
    for tweet in cursor_list:
        counter += 1
        unique_ids.append(tweet["_id"])
        bulk.insert(tweet)

    # move tweets
    bulk.execute()

    # now removed all tweets that were moved over to new database
    bulk_remove = mongo_from.initialize_unordered_bulk_op()
    for one_id in unique_ids:
        bulk_remove.find({"_id": one_id}).remove()

    # # execute operation
    bulk_remove.execute()
    del bulk_remove

    print("*** %20.d finished in: %s" % (user_id, datetime.now() - start_time))

    return counter


total_moved_list = Parallel(n_jobs=-1)(delayed(move_one_user)(one_user_id) for one_user_id in robot_list)

print(np.array(total_moved_list).sum())
