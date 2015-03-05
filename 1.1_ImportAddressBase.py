"""
Description:    Import UK address base
Author:         Bence Komarniczky
Date:           04/March/2015
Python version: 3.4
"""

from ons_twitter.address_import import *
from ons_twitter.data_import import insert_json_mongo
from os import system

# convert large csv file to small JSON files
address_base_loc = "data/input/address/address_base.csv"
address_base = AddressBase("data/output/address/", 200, over_write_previous=True)
insert_list = address_base.import_address_csv(address_base_loc, terminate_at=800)
print(insert_list)

number_files = insert_json_mongo("data/output/address/",
                                 "twitter",
                                 "address")

print(number_files)