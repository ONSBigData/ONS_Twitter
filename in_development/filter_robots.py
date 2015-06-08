"""
Description:    Take out all the robots from the database that have been found so far.
Author: Bence Komarniczky
Date:   08/06/2015
Python version: 3.4
"""

import csv
import pymongo
from datetime import datetime
from joblib import delayed, Parallel

# point to files, with user_ids for robots
recent_robots = "../special_csv/robots.csv"
manual_robots = "../special_csv/manual_robots.csv"
found_robots = "../special_csv/found_robots_1000.csv"

# initialise empty set
robot_list = set([])


def read_one_file(file_path, user_col):
    """
    Read one csv file, containing robot user_ids and add to the
    set of global robots.
    :param file_path: Pointer to file.
    :param user_col: Pythonian index of csv column containing user_ids.
    :return: nothing
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
robot_list = sorted(robot_list)


test_id = robot_list[0]
db = pymongo.MongoClient("192.168.0.99:30000")["twitter"]["tweets"]
db_robots = pymongo.MongoClient("192.168.0.99:30000")["twitter"]["robots"]


def move_one_user(user_id, from_data_base=db, to_data_base=db_robots):
    """
    Move one user to the new robots database.
    :param user_id: user id of robot user
    :param from_data_base: source database
    :param to_data_base: destination database
    :return: number of tweets moved
    """

    # assert input types
    assert type(user_id) == int
    assert type(from_data_base) == pymongo.collection.Collection
    assert type(to_data_base) == pymongo.collection.Collection

    # show user_id to be moved
    print("%20.d  time: %s" % (user_id, datetime.now()))

    # query database and set up bulk insert
    cursor = from_data_base.find({"chunk_id": user_id % 1000, "user_id": user_id})
    bulk = to_data_base.initialize_unordered_bulk_op()

    # initialise list to record unique _id values
    unique_ids = []

    # iterate over cursor
    counter = 0
    for tweet in cursor:
        counter += 1
        unique_ids.append(tweet["_id"])
        # bulk.insert(tweet)

    # move tweets
    # bulk.execute()
    del cursor, bulk

    # now removed all tweets that were moved over to new database
    # bulk_remove = from_data_base.initialize_unordered_bulk_op()
    # for one_id in unique_ids:
    #     bulk_remove.find({"_id": one_id}).remove()
    #
    # # execute operation
    # bulk_remove.execute()
    # del bulk_remove

    return counter

total_moved_list = Parallel(n_jobs=-1)(delayed(move_one_user)(one_user_id) for one_user_id in robot_list[1:8])

print(total_moved_list.sum())
