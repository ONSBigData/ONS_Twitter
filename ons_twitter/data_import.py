"""
Description:    Functions for importing Twitter data from either .csv format or JSON documents,
                as supplied by the Twitter API or GNIP.
Author:         Bence Komarniczky
Date:           02/March/2015
Python version: 3.4
"""

from os import listdir, system
from csv import reader, writer, QUOTE_NONNUMERIC
from datetime import datetime

from pymongo.errors import DuplicateKeyError
import pymongo
import numpy as np
from joblib import Parallel, delayed

from ons_twitter.data_formats import Tweet
from ons_twitter.supporting_functions import *


def import_csv(source,
               mongo_connection,
               mongo_address,
               header=False,
               debug=False,
               print_progress=0):

    # capture start_time
    start_time = datetime.now()

    # check if source is a directory or file
    csv_list = []
    try:
        # grab filename list
        csv_list = listdir(source)
        one_file = False
    except NotADirectoryError:
        one_file = True

    # if source is a single file then process simply
    if one_file:
        aggregated_results = import_one_csv(source,
                                            mongo_connection=mongo_connection,
                                            mongo_address=mongo_address,
                                            header=header,
                                            debug=debug,
                                            print_progress=print_progress)
    else:
        # process contents of folder using joblib in parallel

        # generate filename list
        file_list = [(source + "/" + x) for x in csv_list]

        # do parallel inserts
        results = Parallel(n_jobs=-1)(delayed(import_one_csv)(filename,
                                                              mongo_connection,
                                                              mongo_address,
                                                              header,
                                                              debug,
                                                              None,
                                                              print_progress) for filename in file_list)
        # aggregate statistics
        aggregated_results = np.sum(results, axis=0)

    # print stats
    print("\n **** \nImporting finished!", datetime.now(), "\n * Imported tweets: ", str(aggregated_results[0]),
          "\n * Non_Geo tweets: ", str(aggregated_results[1]),
          "\n * Non_GB tweets: ", str(aggregated_results[2]),
          "\n * Failed tweets: ", str(aggregated_results[3]),
          "\n * Converted no_geo: ", str(aggregated_results[4]),
          "\n * No address found: ", str(aggregated_results[5]),
          "\n * Duplicates: ", str(aggregated_results[6]),
          "\n\n Total time: ", datetime.now() - start_time,
          "\n *****")

    return aggregated_results


def import_one_csv(csv_file_name,
                   mongo_connection=None,
                   mongo_address=None,
                   header=False,
                   debug=False,
                   debug_rows=None,
                   print_progress=0):
    """
    Import one csv file of tweets into a mongodb database while looking up addresses from a mongodb address base

    :param csv_file_name:       location on csv file containing tweets
    :param mongo_connection:    list of mongodb database parameters (ip:host, database, collection) to
                                the twitter database
    :param header:              if true, then csv files contain headers and these need to be skipped
    :param mongo_address:       list of mongodb database parameters (ip:host, database, collection) to a geo_indexed
                                mongodb address base
    :param print_progress:      integer specifying the number of reads at which diagnostics should be
                                printed. 0 will print no diagnostics
    :return:                    numpy array of number of
                                read_tweets/no_geo tweets/non_gb and failed_tweets/
                                 converted_no_geo/ no_address / duplicates
                                successfully converted tweets (no geo -> geo),
                                prints diagnostics and inserts into database
    """
    # set up debug if necessary
    if debug and debug_rows is None:
        debug_rows = 5

    # convert list of mongo connection parameters into mongo connections
    mongo_connection = pymongo.MongoClient(mongo_connection[0], w=1)[mongo_connection[1]][mongo_connection[2]]
    mongo_address = pymongo.MongoClient(mongo_address[0])[mongo_address[1]][mongo_address[2]]

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
        no_address = []

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
                new_tweet = Tweet(row, method="csv")

                if debug:
                    # print tweet before finding address
                    print("\n\n **** Tweet read in ")
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
                    # if all is good then find closest address
                    found_address = new_tweet.find_tweet_address(mongo_address)

                    # if there are no address then keep track of it
                    if found_address == 1:
                        no_address.append(row)

                    # separate tweet into different category if columns have been moved successfully
                    if new_tweet.get_errors() == 2:
                        converted_no_geo.append(row)

                    # print debug info
                    if debug:
                        print("\n\n *** Tweet after address matching!", found_address)
                        new_tweet.get_info()

                    # add tweet to final list
                    read_tweets.append(new_tweet)

                # print progress if needed
                if print_progress > 0:
                    if index % print_progress == 0:
                        print(index, datetime.now())

    # write failed tweets if any
    dump_errors(failed_tweets, "failed_tweets", csv_file_name)

    # dump non-geo located tweets if any
    dump_errors(no_geo, "no_geo", csv_file_name)

    # dump all non GB tweets
    dump_errors(non_gb, "non_GB", csv_file_name)

    # dump successfully converted non_GBs. These will still be processed!
    dump_errors(converted_no_geo, "successful_non_geo", csv_file_name)

    # dump no address tweets. Will still go into pipeline!
    dump_errors(no_address, "no_address_found", csv_file_name)

    # put correct tweets into specified mongo_db database
    duplicates = []
    for tweet in read_tweets:
        try:
            mongo_connection.insert(tweet.dictionary)
        except DuplicateKeyError:
            duplicates.append(tweet.get_csv_format())

    # dump all duplicate tweets
    dump_errors(duplicates, "duplicates", csv_file_name)
    print("Finished", csv_file_name, datetime.now())

    return np.array([len(read_tweets) - len(duplicates), len(no_geo),
                     len(non_gb), len(failed_tweets),
                     len(converted_no_geo), len(no_address), len(duplicates)], dtype="int32")


def create_partition_csv(input_csv,
                         output_folder=None,
                         num_rows=-1,
                         chunk_size=10000,
                         header=False):
    """
    Create a new csv file with a subset of tweets from original raw data.
    Use for debugging code.
    Code will skip the first row to account for possible header row.
    :param input_csv: location of raw tweets to be inserted into mongodb
    :param output_folder:  location of subset of tweets that can be used for testing. If not specified then will output
                        to same folder with "_test_subset" appended.
    :param num_rows: number of tweets in new test dataset. Default is 1000.
    :param chunk_size: number of rows in each chunk. If not zero then will create more files in directory.
    :return: number of tweets written
    """

    # create switch
    if num_rows <= 0:
        dont_stop = True
    else:
        dont_stop = False

    # check if chunks are required
    if chunk_size == 0:
        do_chunks = False
        chunk_size = 1
    else:
        do_chunks = True

    # check if output is specified
    if output_folder is None:
        output_csv = input_csv[:-4] + "_test_subset"
    else:
        create_folder(output_folder)
        output_csv = output_folder

    chunk_index = 0

    # open reader
    with open(input_csv, 'r') as read_input:
        in_tweets = reader(read_input, delimiter=",")

        # initiate list to collect tweets and start indexing
        index = 0
        tweets = []
        for row in in_tweets:
            if index == 0 and header:
                index += 1
                continue
            elif index < num_rows or dont_stop:
                index += 1
                tweets.append(row)

                if (index % chunk_size == 0) and do_chunks:
                    output_csv_name = output_csv + find_file_name(input_csv)[1][:-4] + "_" + str(chunk_index) + ".csv"

                    # start writing tweets
                    with open(output_csv_name, 'w', newline="\n") as out_csv:
                        out_tweets = writer(out_csv, delimiter=",", quoting=QUOTE_NONNUMERIC)
                        out_tweets.writerows(tweets)
                    chunk_index += 1
                    tweets.clear()
            else:
                break

        # start writing tweets
        if len(tweets) != 0:
            output_csv_name = output_csv + str(chunk_index) + ".csv"
            with open(output_csv_name, 'w', newline="\n") as out_csv:
                out_tweets = writer(out_csv, delimiter=",", quoting=QUOTE_NONNUMERIC)
                out_tweets.writerows(tweets)

    return len(tweets)


def dump_errors(dumped_list,
                error_type,
                input_file,
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
        create_folder(output_folder + error_type + "/")
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


def insert_json_mongo(folder_name,
                      database,
                      collection,
                      mongo_ip="127.0.0.1:27017",
                      upsert=False):
    """
    Import all JSON files to a specified mongodb server into database.collection.

    :param folder_name:     Folder containing all JSON objects to be imported.
    :param database:        Name of database where import will be made.
    :param collection:      Name of collection to be updated.
    :param mongo_ip:        IP address of mongo host (with port number). Base value: local mongo at
                            127.0.0.1:27017
    :param upsert:          If true then import will be made with upsert. See mongodb documentation.
    :return:                Number of files inserted.
    """

    # find all files in directory
    file_names = listdir(folder_name)

    # construct command wrapper
    if upsert:
        wrapper = "mongoimport --host %s -d %s -c %s --jsonArray --upsert --file " % (mongo_ip, database, collection)
    else:
        wrapper = "mongoimport --host %s -d %s -c %s --jsonArray --file " % (mongo_ip, database, collection)

    start_time = datetime.now()
    print("\n\n ***** Start mongodb inserts ****", start_time)

    # run all commands
    for file_name in file_names:
        print("\n **** Starting insert: ", datetime.now())
        print(wrapper + folder_name + file_name)
        system(wrapper + folder_name + file_name)

    print("\nFinished inserts in: ", datetime.now() - start_time)
    return len(file_names)