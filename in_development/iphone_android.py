__author__ = 'Bence Komarniczky'
"""
Purpose: Count the number of iphone users/tweets in GNIP data.
Date: 28/04/2015
"""

import json
import os
import gzip
from datetime import date, timedelta, datetime

import pandas as pd


folder = "/nas/data/Twitter Data/newdata/Complete Aug_Oct/"


def read_tweets(file_path):
    infile = gzip.open(file_path, "r")
    file_content = infile.readlines()
    local_tweets = []

    for line in file_content:
        try:
            new_tweet = json.loads(line.decode("utf-8"))
            if len(new_tweet) > 1:
                local_tweets.append(new_tweet)
        except ValueError:
            # print("Conversion failed!", line)
            continue

    infile.close()
    return local_tweets


def create_table(start, end):
    start_date = date(start[0], start[1], start[2])
    end_date = date(end[0], end[1], end[2])
    time_series = []

    while start_date <= end_date:
        time_series.append(start_date)
        start_date += timedelta(days=1)

    returned_table = pd.DataFrame(0, index=time_series,
                                  columns=("iPhone", "iPad", "iOS", "Android", "Windows Phone", "BlackBerry",
                                           "Virtual Jukebox", "Twitter Web", "Instagram", "Tweetbot", "Mac",
                                           "other", "total"))

    return returned_table


def count_sources(tweet_list, data_table):
    for tweet in tweet_list:
        this_date = datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y").date()
        source = tweet["source"]

        found_something = False
        data_table["total"][this_date] += 1

        for search_this in data_table.columns.values[:-1]:
            if source.find(search_this) > 0:
                found_something = True
                data_table[search_this][this_date] += 1
                break

        if not found_something:
            data_table["other"][this_date] += 1

    return data_table


my_table = create_table((2014, 8, 15), (2014, 10, 31))

for file in os.listdir(folder):
    filename = folder + file
    print(filename)
    tweets = read_tweets(filename)
    my_table = count_sources(tweets, my_table)

my_table.to_csv("sources.csv")