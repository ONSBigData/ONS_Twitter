"""
Description:    All small supporting functions that are used cross modules in the Twitter project.
Author:         Bence Komarniczky
Date:           04/March/2015
Python version: 3.4
"""

from os import path, makedirs

# TODO comment and optimise supporting functions

def find_file_extension(file_name):
    """
    Returns the extension of file_name supplied as string.
    :param file_name: String of a file name.
    :return:    File extension type as a string.
    """
    start_index = -1
    found = 0
    while found >= 0:
        found = file_name.find(".", start_index + 1)
        if found >= 0:
            start_index = found

    return file_name[start_index:]


def find_file_name(file_name):
    """
    Find the containing folder and the file name from an input string.
    :param file_name: String pointing to file.
    :return: Tuple of (folder, file)
    Example: data/input/test.csv -> data/input/, test.csv
    """
    start_index = -1
    found = 0
    while found >= 0:
        found = file_name.find("/", start_index + 1)
        if found >= 0:
            start_index = found
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