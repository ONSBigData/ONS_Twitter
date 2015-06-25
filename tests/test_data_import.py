"""
Description:    Test for all the functions in the data_import file.
Author:         Bence Komarniczky
Date:           24/06/2015
Python version: 3.4
"""

import ons_twitter.data_import as data_import


RUN_THESE_TESTS = {"import_csv"}
mongo_address = ("192.168.0.98:30001", "twitter", "address")
test_mongo = ("127.0.0.1:27017", "lib_test")

test_output = "data/output/"

"""
options: "all", "import_csv" , "dump_csv", "dump_json"
"""

test_list = [[1.22, 3.5, "mike test", "en", "", "Isle of Wight", "GB", "50.63", "-1.19", "Happy tonight"],
             [2.11, 4.6, "your_name", "en", "South London ", "Leicester", "GB", "", "", "Every little thing"]]

test_dict = [{"north": 1.22, "east": 2.3, "name": "Lebron James"},
             {"north": 1.35, "east": 3.5, "name": "Kevin Durant"}]

# test_json_path = "data/5minute.json"
#
# test = json.load(open(test_json_path, encoding="utf8"))
#
# with open(test_json_path, encoding="utf8") as in_json:
# for line in in_json:
#         if line != "\n":
#             print(json.loads(line))

if "import_csv" in RUN_THESE_TESTS or "all" in RUN_THESE_TESTS:
    twitter_mongo = (test_mongo[0], test_mongo[1], "csv_import")
    data_import.import_file("data/subset.csv",
                            mongo_connection=twitter_mongo,
                            mongo_address=mongo_address,
                            debug=True)

if "dump_csv" in RUN_THESE_TESTS or "all" in RUN_THESE_TESTS:
    data_import.dump_errors(test_list, "test_csv", "hello/test/1.csv", test_output + "csv_dump/")

if "dump_json" in RUN_THESE_TESTS or "all" in RUN_THESE_TESTS:
    data_import.dump_errors(test_dict, "test_j", "hello/test/1.json", test_output + "csv_dump/")

