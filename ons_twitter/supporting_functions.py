"""
Description:    All small supporting functions that are used cross modules in the Twitter project.
Author:         Bence Komarniczky
Date:           04/March/2015
Python version: 3.4
"""

from os import path, makedirs


def find_file_extension(file_name):
    """
    Returns the extension of file_name supplied as string.

    :param file_name:   File name.
    :return:            File extension type as a string. Eg: .csv, .json

    :type file_name     str
    :rtype              str
    """

    # set start index so that if no extension is found then return the whole filename
    start_index = 0

    # look for the last dot in the file name
    for i in range(len(file_name)-1, 0, -1):
        if file_name[i] == ".":
            start_index = i
            break

    # return file extension
    return file_name[start_index:]


def find_file_name(file_name):
    """
    Find the containing folder and the file name from an input string.
    :param file_name: String pointing to file.
    :return: Tuple of (folder, file)
    Example: data/input/test.csv -> data/input/, test.csv
    """

    #TODO continue from here
    # set start index so that if no extension is found then return the whole filename
    start_index = 0

    # look for the last / in the file name
    for i in range(len(file_name)-1, 0, -1):
        if file_name[i] == "/":
            start_index = i
            break
    return file_name[:start_index + 1], file_name[(start_index + 1):]


def create_folder(folder_loc):
    """
    Create folder in system recursively. i.e. create one if one doesn't exist
    :param folder_loc: String pointing to the location of new folder.
    :return:
    """
    if not path.exists(folder_loc):
            makedirs(folder_loc)


def distance(point1,
             point2):
    """
    Given two tuples or lists, returns the distance between the two points, rounded to 3 decimal places.
    :param point1: First point of coordinates. (Tuple/list)
    :param point2: Second point of coordinates. (Tuple/list)
    :return: float
    """

    euclidean_squared = ((point1[0] - point2[0]) ** 2) + ((point1[1] - point2[1]) ** 2)
    return round(euclidean_squared ** 0.5, 3)