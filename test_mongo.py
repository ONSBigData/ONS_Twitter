"""
Description: Test performance of WiredTiger
Author: Bence Komarniczky
Date: 27/03/2015
Python version: 3.4
"""

import pymongo
import random
from datetime import datetime
from joblib import Parallel, delayed

amount = 10000000
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





start_time = datetime.now()

db = pymongo.MongoClient()["test"]["performance"]
bulk = db.initialize_unordered_bulk_op()
for item in insert_these:
    bulk.insert(item)

bulk.execute()

print("Inserts: ", amount / (datetime.now()- start_time).microseconds)
start_updates = datetime.now()

bulk = db.initialize_unordered_bulk_op()
for item_index in range(len(update_these)):
    bulk.find(insert_these[item_index]).update({"$set": update_these[item_index]})

bulk.execute()


elapsed_time = datetime.now()-start_updates
print("Update performance: ", amount / elapsed_time.microseconds)



# test with 20,000 inserts
# 0.025 inserts/microseconds with no bulking
# 0.024 inserts with ordered bulking
# 0.057 inserts/microseconds with unordered bulking      ---- updates:  0.023

# updates with padding: 0.048



# test with 100,000 documents:
# one field, padded bulked: 0.16/0.3 (insert/update)

# multiple fields: very varied (0.2-2.3 / 0.2-1.2)

# with 3.0: 0.15-3 / 0.12-0.25

# with more documents: 21.8 inserts per microseconds - wiredtiger

# mongod --storageEngine=wiredTiger