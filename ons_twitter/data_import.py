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
from json import dump

from pymongo.errors import DuplicateKeyError
import pymongo
import numpy as np
from joblib import Parallel, delayed

from ons_twitter.data_formats import Tweet
from ons_twitter.supporting_functions import *


def import_file(source,
                mongo_connection,
                mongo_address,
                header=False,
                debug=False,
                print_progress=0):
    """
    Function imports a list of csv files containing tweets into mongodb database. For each tweet, the function finds
    its closest address point (within 300m) and then creates a dictionary of tweet information. This information is
    then inserted into the database.

    :param source:              Folder of CSV/JSON files. (JSON not working yet.) Can be single file as well.
    :param mongo_connection:    Targeted database.
    :param mongo_address:       List of mongodb address base databases.
    :param header:              Boolean indicating presence of header row.
    :param debug:               Boolean for debugging.
    :param print_progress:      Integer specifying intensity of verbosity. (Print at this many lines.)
    :return:                    Aggregated results from all files imported.
                                Imported/Non_Geo/Non_GB/Failed/converted/no address/mongo_errors
    """

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
        aggregated_results = import_one_file(source,
                                             mongo_connection=mongo_connection,
                                             mongo_address=mongo_address,
                                             header=header,
                                             debug=debug,
                                             print_progress=print_progress)
    else:
        # process contents of folder using joblib in parallel

        # generate filename list
        file_list = [(source + "/" + x) for x in csv_list]

        # decide on parallel mongodb lookup
        if type(mongo_address[0]) is str:
            results = Parallel(n_jobs=-1)(delayed(import_one_file)(filename,
                                                                   mongo_connection,
                                                                   mongo_address,
                                                                   header,
                                                                   debug,
                                                                   None,
                                                                   print_progress) for filename in file_list)
        else:
            # verbose
            print("\nMore than one address base were supplied!",
                  "\nUsing all of them:\n")
            for one_address in mongo_address:
                print(one_address)

            print("*****\n")

            # create an iterable
            dummy_mongo = mongo_address * ((len(file_list) // len(mongo_address)) + 1)
            """:type : list"""
            dummy_mongo = dummy_mongo[:len(file_list)]
            mongo_chunk_iter = []

            i = 0
            for address_param in dummy_mongo:
                mongo_chunk_iter.append((file_list[i], address_param))
                i += 1

            # call parallel
            results = Parallel(n_jobs=-1)(delayed(import_one_file)(param[0],
                                                                   mongo_connection,
                                                                   param[1],
                                                                   header,
                                                                   debug,
                                                                   None,
                                                                   print_progress) for param in mongo_chunk_iter)

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
          "\n * Mongo Errors: ", str(aggregated_results[7]),
          "\n\n Total time: ", datetime.now() - start_time,
          "\n *****")

    return aggregated_results


def import_one_file(csv_file_name,
                    mongo_connection=None,
                    mongo_address=None,
                    header=False,
                    debug=False,
                    debug_rows=None,
                    print_progress=0):
    """
    Import one csv file of tweets into a mongodb database while looking up addresses from a mongodb address base.
    Invalid tweets will be filtered into a separate folder.

    :param csv_file_name:       location on csv file containing tweets
    :param mongo_connection:    list of mongodb database parameters (ip:host, database, collection) to
                                the twitter database. Can also be a list of mongodb databases.
    :param header:              if true, then csv files contain headers and these need to be skipped
    :param mongo_address:       list of mongodb database parameters (ip:host, database, collection) to a geo_indexed
                                mongodb address base
    :param print_progress:      integer specifying the number of reads at which diagnostics should be
                                printed. 0 will print no diagnostics
    :return:                    numpy array with number of
                                inserted, no_geo, non_GB, failed, converted, no_address, duplicate, mongo_error tweets
    """
    # set up debug if necessary
    if debug and debug_rows is None:
        debug_rows = 5

    # TODO implement json imports

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
        mongo_error = []

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
                    elif found_address == 2:
                        # if there was a mongo error then do not add to database
                        mongo_error.append(row)
                        continue

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

    # dump mongo errors, won't be in database
    dump_errors(mongo_error, "mongo_error", csv_file_name)

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

    # return insert statistics
    return np.array([len(read_tweets) - len(duplicates), len(no_geo),
                     len(non_gb), len(failed_tweets),
                     len(converted_no_geo), len(no_address), len(duplicates), len(mongo_error)], dtype="int32")


def create_partition_csv(input_csv,
                         output_folder=None,
                         num_rows=-1,
                         chunk_size=10000,
                         header=False):
    """
    Create a set of new csv files in the output folder from the input_csv. Use the chunk_size parameter
    to create smaller chunks of the original data. This is useful for debugging.

    :param input_csv:       Location of raw tweets to be split
    :param output_folder:   Location of subset of csv files that can be used for testing. If not specified then will output
                            to same folder with "_subset" appended.
    :param num_rows:        Number of tweets in new test dataset. Default is 10000.
    :param chunk_size:      Number of rows in each chunk. If positive then function will create a set of smaller files.
    :return:                Number of rows written to new files.

    :type input_csv         str
    :type output_folder     str or None
    :type num_rows          int
    :type chunk_size        int
    :type header            bool
    :rtype                  int
    """

    # check if chunks are required
    if chunk_size <= 0:
        do_chunks = False
        chunk_size = 1
    else:
        do_chunks = True

    # check if output is specified
    if output_folder is None:
        output_csv = "%s_" % input_csv[:-4]
    else:
        create_folder(output_folder)
        output_csv = output_folder

    # initialise chunk index
    chunk_index = 0

    # open file for splitting
    with open(input_csv, 'r') as read_input:
        # read with csv module
        in_tweets = reader(read_input, delimiter=",")

        # initiate list to collect tweets and start indexing
        index = 0
        tweets = []

        for row in in_tweets:
            # skip header if specified
            if index == 0 and header:
                index += 1
                continue

            index += 1
            tweets.append(row)

            # if chunk limit is reached then split out new csv
            if (index % chunk_size == 0) and do_chunks:

                # create new name for csv
                output_csv_name = "%s%s_%06d.csv" % (output_csv, find_file_name(input_csv)[1][:-4], chunk_index)

                # start writing tweets
                with open(output_csv_name, 'w', newline="\n") as out_csv:
                    out_tweets = writer(out_csv, delimiter=",", quoting=QUOTE_NONNUMERIC)
                    out_tweets.writerows(tweets)

                # clear tweets and add increase chunk index
                chunk_index += 1
                tweets.clear()

            # if break point (num_rows) is reached then stop
            if index == num_rows:
                break

    # write out any remaining tweets
    if len(tweets) != 0:
        output_csv_name = "%s%s_%06d.csv" % (output_csv, find_file_name(input_csv)[1][:-4], chunk_index)
        with open(output_csv_name, 'w', newline="\n") as out_csv:
            out_tweets = writer(out_csv, delimiter=",", quoting=QUOTE_NONNUMERIC)
            out_tweets.writerows(tweets)
        tweets.clear()

    # return the number of tweets written to disk
    return index - 1 if header else index


def dump_errors(dump_this_data,
                error_type,
                input_file,
                output_folder="data/output/errors/"):
    """
    Dumps errors from a list to a new file. A list of dictionaries is dumped as a json file
    while a list of lists is dumped as csv file (each list is a row).
    The file will go into a subdirectory of the output folder, based on  the error type. This
    helps with keeping track of your errors.

    :param dump_this_data:  List of errors to dump, can be empty for no action.
    :param error_type:      Type of error, will generate new folder for each type.
    :param input_file:      Name of input file. Function keeps track of this, by
                            appending name to output file. Again for quality control.
    :param: output_folder:  Folder path for all errors.
    :return:                Number of dumped errors.

    :type dump_this_data    list
    :type error_type        str
    :type input_file        str
    :type output_folder     str
    :rtype                  int
    """

    assert type(dump_this_data) in (list, tuple), "dump_this_data must be iterable"

    # check if there are any documents to be dumped
    if len(dump_this_data) == 0:
        return 0

    assert type(dump_this_data[0]) is list or type(dump_this_data[0]) is dict, "dump_this_data must be an iterable of" \
                                                                               "dictionaries or lists"

    # get the type of the input file
    file_ext = find_file_extension(input_file).lower()

    # create folder if needed
    create_folder(output_folder + error_type + "/")

    # specify beginning of filename
    outfile_beginning = "%s%s/%s_%s" % \
                        (output_folder,
                         error_type,
                         find_file_name(input_file)[1][:-len(file_ext)],
                         error_type)

    # infer type of input from type, list -> csv and dict -> json
    if type(dump_this_data[0]) is dict:
        # add file extension
        outfile = outfile_beginning + ".json"

        # dump json
        with open(outfile, "w", newline="\n") as out_file:
            dump(dump_this_data, out_file, sort_keys=True, indent=4)

        return len(dump_this_data)

    elif type(dump_this_data[0]) is list:
        # add file extension
        outfile = outfile_beginning + ".csv"

        # write to csv
        with open(outfile, "w", newline="\n") as out_file:
            writing_files = writer(out_file, quoting=QUOTE_NONNUMERIC, delimiter=",")
            writing_files.writerows(dump_this_data)

        return len(dump_this_data)


def insert_json_mongo(folder_name,
                      mongo_connection,
                      upsert=False):
    """
    Import all JSON files to a specified mongodb server into database.collection. This uses
    command line arguments instead of pymongo, so will not give any errors or other output.
    Caution!!! Only use for importing address base into completely empty database.

    :param folder_name:     Folder containing all JSON objects to be imported.
    :param mongo_connection:Usual list of mongodb connection parameters. (ip:host, database, collection)
    :param upsert:          If true then import will be made with upsert. See mongodb documentation.
    :return:                Number of files inserted.


    :type folder_name       str
    :type mongo_connection  list or tuple
    :type upsert            bool
    :rtype                  int
    """

    assert type(mongo_connection) is tuple or type(mongo_connection) is list, "Mongo connection must be a tuple or list"
    assert len(mongo_connection), "Mongo connection must be of form (ip:host, database, collection)"

    # find all files in directory
    file_names = listdir(folder_name)

    # deconstruct mongodb connection tuple
    mongo_ip = mongo_connection[0]
    database = mongo_connection[1]
    collection = mongo_connection[2]

    # construct command wrapper
    if upsert:
        wrapper = "mongoimport --host %s -d %s -c %s --jsonArray --upsert --file " % (mongo_ip, database, collection)
    else:
        wrapper = "mongoimport --host %s -d %s -c %s --jsonArray --file " % (mongo_ip, database, collection)

    start_time = datetime.now()
    print("\n\n ***** Start mongodb inserts **** at: %s" % start_time)

    # run all commands
    for file_name in file_names:
        print("\n **** Starting insert: %s" % datetime.now())
        print(wrapper + folder_name + file_name)
        system(wrapper + folder_name + file_name)

    print("\nFinished inserts in: %s" % (datetime.now() - start_time))
    return len(file_names)