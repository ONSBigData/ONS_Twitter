"""
Description:    Import UK address base
Author:         Bence Komarniczky
Date:           04/March/2015
Python version: 3.4
"""

from ons_twitter.data_formats import *
from ons_twitter.data_import import insert_json_mongo

# specify constants
address_base_csv_location = "data/input/address/address_base.csv"
address_json_folder = "data/output/address/"

mongo_ip = "192.168.0.98:30001"
database_name = "twitter"
collection_name = "address"

# verbose
print("\n ***** Start converting csv to JSON", "\nfrom: ", address_base_csv_location,
      "\ninto : ", address_json_folder, "\n", datetime.now())

# convert large csv file to small JSON files
address_base = AddressBase(folder_name=address_json_folder,
                           chunk_size=1000,
                           over_write_previous=True)

insert_list = address_base.import_address_csv(input_file_location=address_base_csv_location,
                                              terminate_at=-1)

print("\nJSON conversion finished  ", datetime.now(),
      "\ncreated a total of ", len(insert_list), "files")

# import into mongodb database
number_files = insert_json_mongo(folder_name=address_json_folder,
                                 database=database_name,
                                 collection=collection_name,
                                 mongo_ip=mongo_ip)