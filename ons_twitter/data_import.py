"""
Description:    Functions for importing Twitter data from either .csv format or JSON documents,
                as supplied by the Twitter API or GNIP.
Author:         Bence Komarniczky
Date:           02/March/2015
Python version: 3.4
"""

from os import listdir
from csv import QUOTE_MINIMAL, reader, writer
import ons_twitter.data_formats as df


test_file = "C:/Users/ONS-BIG-DATA/Documents/TWITTER/twitter/data/input/Tweets_Apr_Oct_test_subset.csv"


def import_csv(infile, mongo_connection, header= False):
    try:
        csv_list = listdir(infile)
        one_file = False
    except NotADirectoryError:
        one_file = True


def import_one_csv(csv_file_name, mongo_connection=None, header=False, debug=False):
    """
    Import one csv file of tweets into a mongodb database

    :param csv_file_name: location on csv file containing tweets
    :param mongo_connection: mongodb pointer to database (i.e. connection.db.collection)
    :param header: if true, then csv files contain headers and these need to be skipped
    :return:    tuple of number of read_tweets/no_geo tweets and failed_tweets,
                prints diagnostics and inserts into database
    """
    # start reading csv file
    with open(csv_file_name, 'r') as in_tweets:
        # set up csv reader
        input_rows = reader(in_tweets, delimiter=",")

        # start indexing, initiate lists for collecting tweets
        index = 0
        read_tweets = []
        no_geo = []
        failed_tweets = []

        # iterate over each row of input csv
        for row in input_rows:
            if header and index == 0:
                header_row = row
                index += 1
                continue
            else:
                index += 1
                new_tweet = df.Tweet(row, method="csv")

                if debug:
                    new_tweet.get_info()
                    if index == 6:
                        break

                # check if any errors occured
                if new_tweet.get_errors() == 1:
                    # change this to csv output
                    no_geo.append(row)
                elif new_tweet.get_errors() == 2:
                    # change this to csv output
                    failed_tweets.append(row)
                else:
                    read_tweets.append(new_tweet)

    # write failed tweets if any
    if len(failed_tweets) != 0:
        dump_erros = csv_file_name[:-4] + "_errors.csv"
        with open(dump_erros, 'w', newline="\n") as dump_tweets:
            out_csv = writer(dump_tweets, delimiter=",", quoting=QUOTE_MINIMAL)
            out_csv.writerows(failed_tweets)

    # dump non-geo located tweets if any
    if len(no_geo) != 0:
        dump_geo = csv_file_name[:-4] + "_no_geo.csv"
        with open(dump_geo, 'w', newline="\n") as dump_tweets:
            out_csv = writer(dump_tweets, delimiter=",", quoting=QUOTE_MINIMAL)
            out_csv.writerows(no_geo)

    return len(read_tweets), len(no_geo), len(failed_tweets)


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
            elif index <= (num_rows):
                index += 1
                tweets.append(row)
            else:
                break

        # start writing tweets
        with open(output_csv, 'w', newline="\n") as out_csv:
            out_tweets = writer(out_csv, delimiter=",", quoting=QUOTE_MINIMAL)
            out_tweets.writerows(tweets)

    return len(tweets)


get_diagnostics = import_one_csv(test_file, header=False)
print(get_diagnostics)

