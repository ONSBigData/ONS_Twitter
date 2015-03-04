"""
Description:    Functions for importing Twitter data from either .csv format or JSON documents,
                as supplied by the Twitter API or GNIP.
Author:         Bence Komarniczky
Date:           02/March/2015
Python version: 3.4
"""

from os import listdir, path, makedirs
from csv import reader, writer, QUOTE_NONNUMERIC
import ons_twitter.data_formats as df
from datetime import datetime


def import_csv(infile, mongo_connection, header=False):
    try:
        csv_list = listdir(infile)
        one_file = False
    except NotADirectoryError:
        one_file = True


def import_one_csv(csv_file_name, mongo_connection=None, mongo_address=None, header=False, debug=False,
                   debug_rows=None, print_progress=False):
    """
    Import one csv file of tweets into a mongodb database

    :param csv_file_name: location on csv file containing tweets
    :param mongo_connection: mongodb pointer to database (i.e. connection.db.collection)
    :param header: if true, then csv files contain headers and these need to be skipped
    :param mongo_address: pointer to mongodb database with geo_indexed address base
    :return:    tuple of number of read_tweets/no_geo tweets/non_gb and failed_tweets +
                successfully converted tweets (no geo -> geo),
                prints diagnostics and inserts into database
    """
    # set up debug if necessary
    if debug and debug_rows is None:
        debug_rows = 5

    # start reading csv file
    with open(csv_file_name, 'r') as in_tweets:
        # set up csv reader
        input_rows = reader(in_tweets, delimiter=",")

        # start indexing, initiate lists for collecting tweets
        index = 0
        read_tweets = []
        no_geo = []
        converted_no_geo = []
        failed_tweets = []
        non_gb = []

        # iterate over each row of input csv
        for row in input_rows:
            if header and index == 0:
                header_row = row
                if debug:
                    print("\nHeader row: ")
                    print(header_row)
                    print("\n ***")
                index += 1
                continue
            else:
                index += 1
                new_tweet = df.Tweet(row, method="csv")

                if debug:
                    new_tweet.get_info()
                    if index == debug_rows:
                        break
                if debug_rows is not None:
                    if index == debug_rows + 1:
                        break

                # check if any errors occurred
                if new_tweet.get_errors() in (1, 3):
                    # change this to csv output
                    no_geo.append(row)
                elif new_tweet.get_errors() == -1:
                    # change this to csv output
                    failed_tweets.append(row)
                elif new_tweet.get_country_code() != "GB":
                    non_gb.append(row)
                else:
                    #if all is good then find closest address
                    found_address = new_tweet.find_tweet_address(mongo_address)
                    if new_tweet.get_errors() == 2:
                        converted_no_geo.append(row)
                    if debug:
                        print("Address found: ", found_address)
                        new_tweet.get_info()
                    read_tweets.append(new_tweet)
                    pass

                if print_progress:
                    if index % 10000 == 0:
                        print(index, datetime.now())

    # write failed tweets if any
    dump_errors(failed_tweets, "failed_tweets", csv_file_name)

    # dump non-geo located tweets if any
    dump_errors(no_geo, "no_geo", csv_file_name)

    # dump all non GB tweets
    dump_errors(non_gb, "non_GB", csv_file_name)

    # dump successfully converted non_GBs. These will still be processed!
    dump_errors(converted_no_geo, "successful_non_geo", csv_file_name)

    return len(read_tweets), len(no_geo), len(non_gb), len(failed_tweets), len(converted_no_geo)


def create_test_csv(input_csv, output_csv=None, num_rows=1000):
    """
    Create a new csv file with a subset of tweets from original raw data.
    Use for debugging code.
    Code will skip the first row to account for possible header row.
    :param input_csv: location of raw tweets to be inserted into mongodb
    :param output_csv:  location of subset of tweets that can be used for testing. If not specified then will output
                        to same folder with "_test_subset" appended.
    :param num_rows: number of tweets in new test dataset. Default is 1000.
    :return: number of tweets written
    """

    # check if output is specified
    if output_csv is None:
        output_csv = input_csv[:-4] + "_test_subset.csv"

    # open reader
    with open(input_csv, 'r') as read_input:
        in_tweets = reader(read_input, delimiter=",")

        # initiate list to collect tweets and start indexing
        index = 0
        tweets = []
        for row in in_tweets:
            if index == 0:
                index += 1
                continue
            elif index <= num_rows:
                index += 1
                tweets.append(row)
            else:
                break

        # start writing tweets
        with open(output_csv, 'w', newline="\n") as out_csv:
            out_tweets = writer(out_csv, delimiter=",", quoting=QUOTE_NONNUMERIC)
            out_tweets.writerows(tweets)

    return len(tweets)


def dump_errors(dumped_list, error_type, input_file,
                output_folder="data/output/errors/"):
    """
    Dumps errors from a list to a new file. Works with both json and csv files.
    Returns the number of objects in the list. If list is empty, then does not write.

    :param dumped_list:     list of errors to dump, can be empty for no action
    :param error_type:      type of error, will generate new folder for each type
    :param input_file:      name of input file. Function keeps track of this, by
                            appending name to output file. If csv then file will be output
                            to csv. If json then a new json file will be created.
    :param: output_folder:  folder for all errors
    :return:                number of dumped objects, -1 if errors occur
    """

    if len(dumped_list) == 0:
        return 0
    else:
        file_ext = find_file_extension(input_file)
        if not path.exists(output_folder + error_type + "/"):
            makedirs(output_folder + error_type + "/")
        if file_ext == ".csv":
            outfile = output_folder + error_type + "/" + find_file_name(input_file)[1][:-len(file_ext)] + \
                "_" + error_type + file_ext
            with open(outfile, "w", newline="\n") as out_file:
                writing_files = writer(out_file, quoting=QUOTE_NONNUMERIC, delimiter=",")
                writing_files.writerows(dumped_list)
        elif file_ext == ".JSON":
            pass
        else:
            print("input file: ", input_file, "\n must be of type .csv or .JSON")
            return -1


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
