"""
Description:    Code for the DBScan clustering algorithm.
Author:         Bence Komarniczky
Date:           09/03/2015
Python version: 3.4
"""

import numpy as np


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
        # get two matrices with all possible point combinations
        m, n = np.meshgrid(all_points, all_points)

        # take the difference of their absolute values, this is the euclidean distance by definition
        distance_array = abs(m - n)

        # convert them into integers in place
        distance_array_integer = distance_array.view('int64')
        distance_array_integer[:] = distance_array

    else:
        # initiate empty matrix
        distance_array_integer = np.empty((n, n), dtype="int32")

        # calculate the number of required blocks
        blocks = n // block_size

        # iterate over block ids
        for row_id in range(blocks + 1):
            # do strips of distances
            m, n = np.meshgrid(all_points[(row_id * block_size):((row_id + 1) * block_size)],
                               all_points)
            distance_array = abs(m - n)

            # convert to integers
            distance_array = distance_array.astype("int32")

            # attach to matrix
            distance_array_integer[:, (row_id * block_size):((row_id + 1) * block_size)] = distance_array

    return distance_array_integer


def create_one_cluster(cluster_points, remaining_mask, distance_array, eps=20):
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

            # search row
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

    return new_cluster, remaining_mask