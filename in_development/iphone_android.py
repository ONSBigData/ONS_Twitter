__author__ = 'Bence Komarniczky'
"""
Purpose: Count the number of iphone users/tweets in GNIP data.
Date: 28/04/2015
"""

import json
import os


all_percents = []
all_tweets = 0

apple_users = {"iOS": 0, "iPad": 0, "iPhone": 0}
for fn in os.listdir("data/oct/"):
    tweets = []

    filename = "data/oct/" + fn
    with open(filename, 'r') as infile:
        for row in infile:
            try:
                len(row)
                new_tweet = json.loads(row)
                if len(new_tweet.keys()) > 2:
                    tweets.append(new_tweet)
            except ValueError:
                continue

    sources = []

    for one_tweet in tweets:
        sources.append(one_tweet["source"])

    all_tweets += len(tweets)
    search_these = ("iOS", "iPad", "iPhone")
    apple_set = set([])

    for source in sources:
        source = str(source)
        for query in search_these:
            if source.find(query) > -1:
                apple_users[query] += 1
                apple_set.add(source)
            else:
                continue

    print(apple_users)
    all_percents.append(apple_users["iPhone"] / len(tweets))

print((apple_users["iPhone"] + apple_users["iOS"] + apple_users["iPad"]) / all_tweets)

print((apple_users["iPhone"]) / all_tweets)
