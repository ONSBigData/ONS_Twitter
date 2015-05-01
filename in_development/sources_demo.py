__author__ = 'Bence Komarniczky'
"""
Purpose: Demonstration of JSON parsing.

Date:
"""

import json
import os
import gzip
from datetime import date, timedelta, datetime

from matplotlib import pyplot
from joblib import Parallel, delayed
import pandas as pd


folder = "/nas/data/Twitter Data/newdata/Complete Aug_Oct/"


def read_tweets(file_path):
    print(file_path, datetime.now())

    infile = gzip.open(file_path, "r")
    file_content = infile.readlines()

    counter = 0
    for line in file_content:
        try:
            new_tweet = json.loads(line.decode("utf-8"))
            if len(new_tweet) > 1:
                if counter == 0:
                    this_date = datetime.strptime(
                        new_tweet["created_at"], "%a %b %d %H:%M:%S %z %Y").date().strftime("%Y,%m,%d")
                    this_date = this_date.split(",")
                    numeric_date = [int(dummy) for dummy in this_date]
                    pandas_table = create_table(numeric_date, numeric_date)

                parse_tweet(new_tweet, pandas_table)
        except ValueError:
            # print("Conversion failed!", line)
            continue
        counter += 1

    infile.close()
    return pandas_table


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


def parse_tweet(tweet, data_table):
    this_date = datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y").date()
    source = tweet["source"]

    found_something = False
    data_table["total"][this_date] += 1

    for search_this in data_table.columns.values[:-2]:
        if source.find(search_this) > 0:
            found_something = True
            data_table[search_this][this_date] += 1
            break

    if not found_something:
        data_table["other"][this_date] += 1


file_names = sorted([(folder + fn) for fn in os.listdir(folder)])[1:200]

start_time = datetime.now()
print("Starting parsing!", start_time)
tables = Parallel(n_jobs=8)(delayed(read_tweets)(file_path) for file_path in file_names)
print("Finished parsing", datetime.now() - start_time)

my_table = tables[0]
for table in tables[1:]:
    my_table = my_table.add(table, fill_value=0)

my_table.to_csv("sources_demo.csv")

# plot the whole data
my_data = pd.read_csv("/VOLUME/twitter/in_development/sources.csv", index_col=0, parse_dates=True)
pyplot.interactive(False)
my_data.plot()
pyplot.show()