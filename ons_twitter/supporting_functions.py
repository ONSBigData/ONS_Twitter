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
    Return a tuple of (folder_name/, file_name)
    Example: data/input/test.csv -> data/input/, test.csv


    :param file_name:   Full file name.
    :return:            Tuple of folder path and filename.

    :type file_name     str
    :rtype              tuple
    """

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
    Create folder if it doesn't exist.

    :param folder_loc:  Location of folder
    :return:            None

    :type folder_loc    str
    :rtype              None
    """

    if not path.exists(folder_loc):
            makedirs(folder_loc)

    return None


def distance(point1, point2):
    """
    Given two tuples or lists, return the distance between the two points,
    rounded to 3 decimal places. Using easting,northing pairs it returns distance in meters.
    :param point1:  First point of coordinates.
    :param point2:  Second point of coordinates.
    :return:        Distance in meters.

    :type point1    tuple[float] | list[float]
    :type point2    tuple[float] | list[float]
    :rtype          float
    """

    euclidean_squared = ((point1[0] - point2[0]) ** 2) + ((point1[1] - point2[1]) ** 2)
    return round(euclidean_squared ** 0.5, 3)
