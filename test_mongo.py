"""
Description: Test performance of WiredTiger
Author: Bence Komarniczky
Date: 27/03/2015
Python version: 3.4
"""

import pymongo
import random
from datetime import datetime

amount = 100000
this_string = "GTHTHFVRDBYNFYNGUONSZRCECGHVUBDXFHGVJNFNDBVTHGVJNYXRBYXRBYTYNTHYFHVBYTFYHVTYFVHCTBHC"
length = len(this_string)

insert_these = []
update_these = []
for x in range(amount):
    selectors = [int(length * random.random()), int(length * random.random())]
    selectors.sort()
    new_string = this_string[selectors[0]: selectors[1]]
    insert_these.append({"a": new_string, "c": "test", "d": 1234343, "f": "haha"})
    update_these.append({"b": new_string[::-1]})

# for x in range(3):
#
#     start_time = datetime.now()
#     db = pymongo.MongoClient()["test"]["performance"]
#     for item in insert_these:
#         db.insert(item)
#
#     elapsed_time = datetime.now()-start_time
#     print("Performance: ", amount / elapsed_time.microseconds, " inserts per microsecond")
#     db.remove({})

for x in range(3):
    start_time = datetime.now()

    db = pymongo.MongoClient()["test"]["performance"]
    bulk = db.initialize_unordered_bulk_op()
    for item in insert_these:
        item["padding"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        bulk.insert(item)
        bulk.find(item).update({"$unset": {"padding": ""}})

    bulk.execute()
    elapsed_time = datetime.now()-start_time
    print("Insert performance: ", amount / elapsed_time.microseconds, " inserts per microsecond")


    db.ensure_index([("a", pymongo.ASCENDING)])

    update_start = datetime.now()

    bulk = db.initialize_ordered_bulk_op()
    for index in range(len(update_these)):
        bulk.find(insert_these[index]).update({"$set": update_these[index]})

    bulk.execute()
    elapsed_time = datetime.now()- update_start
    print("Update performance: ", amount / elapsed_time.microseconds, " updates per microsecond")

    db.remove({})

# test with 20,000 inserts
# 0.025 inserts/microseconds with no bulking
# 0.024 inserts with ordered bulking
# 0.057 inserts/microseconds with unordered bulking      ---- updates:  0.023

# updates with padding: 0.048



# test with 100,000 documents:
# one field, padded bulked: 0.16/0.3 (insert/update)

# multiple fields: very varied (0.2-2.3 / 0.2-1.2)