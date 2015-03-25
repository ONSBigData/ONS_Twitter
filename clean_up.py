__author__ = 'Bence Komarniczky'
"""
Purpose:

Date:
"""
import csv

destination = "data/input/new_data/chunk_clean_up/"

harvested_tweets = "data/input/final_data/Tweets_Apr12_Aug14.csv"
april_tweets = "data/input/final_data/GNIP_April.csv"
aug_oct_tweets = "data/input/final_data/GNIP_August_October.csv"

# put them as tuple
files = (harvested_tweets,
         april_tweets,
         aug_oct_tweets)

all_tweets = []
with open(harvested_tweets, 'r') as at:
    read_tweets = csv.reader(at, delimiter=",")

    index = 0
    for row in read_tweets:
        if index < 62770000:
            index += 1
            continue
        else:
            all_tweets.append(row)
            index += 1

    print(index)

with open(destination + "Tweets_Apr12_Aug14.csv", 'w') as outcsv:
    write_tweets = csv.writer(outcsv, delimiter=",", quoting=csv.QUOTE_NONNUMERIC)
    write_tweets.writerows(all_tweets)