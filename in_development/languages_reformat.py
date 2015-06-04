"""
Description: Reformat the languages file so that users are weighted accordingly.
Author: Bence Komarniczky
Date:   04/06/2015
Python version: 3.4
"""

import pandas as pd

df = pd.read_csv("languages.csv", index_col=[0, 1])

df.drop("user_id", 1, inplace=True)



languages = df.index.get_level_values("languages")

unique_languages = set([])

for one_row in list(languages):
    for one_language in one_row.split("_"):
        unique_languages.add(one_language)

languages_dict = {}
for one_language in unique_languages:
    languages_dict[one_language] = 0


new_df = pd.DataFrame(languages_dict, index=df.index.get_level_values("oslaua").unique(), dtype=float)

counter = 0
for one_row in df.iterrows():

    counter += 1
    if counter % 100 == 0:
        print(counter)


    geography = one_row[0][0]
    langs = one_row[0][1]
    counts = one_row[1]["_id"]

    langs = langs.split("_")

    try:
        langs.remove("und")
    except ValueError:
        continue

    weight = len(langs)

    counts /= weight

    for l in langs:
        new_df[l][geography] += counts


new_df.to_csv("language_matrix.csv")