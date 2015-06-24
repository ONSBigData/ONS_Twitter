"""
Description:    Import UK address base. First slice up large csv file into small JSONs and then
                import the small files into a mongodb database.
Author:         Bence Komarniczky
Date:           04/March/2015
Python version: 3.4
"""

from ons_twitter.data_formats import AddressBase
from ons_twitter.data_import import insert_json_mongo
from datetime import datetime

# specify address base csv location and temporary folder for json files
address_base_csv_location = "data/input/address/address_base.csv"
address_json_folder = "data/output/address/"

'''
The address base csv should be of the format:
POSTCODE: (string) valid UK postcode
UPRN: (int) Unique Property Reference Number as defined by Ordnance Survey
X_COORDINATE: (float/int) Easting coordinate of address point
Y_COORDINATE (float/int) Northing coordinate of address point
CLASSIFICATION_CODE: (string) as in AddressBase
oslaua: (string) local authority code
osward: (string) electoral ward codes
oa11: (string) 2011 Census output area
lsoa11: (string) 2011 lower layer super output area code
msoa11: (string) 2011 middle layer super output area code
wz11: (string) 2011 Workplace Zone code

Note that this structure is hardcoded in the class definition of Address in
ons.twitter.data_formats. If you require a different format then you either have
to change that class or munge your address base csv!
'''

# specify destination mongodb database to hold addresses
mongo_destination = ("192.168.0.98:30001", "twitter", "address")

# print info for user
print("\n ***** Start converting csv to JSON\nfrom: %s\ninto: %s\nat: %s" %
      (address_base_csv_location, address_json_folder, datetime.now()))

# initialise AddressBase object
address_base = AddressBase(folder_name=address_json_folder,
                           chunk_size=1000,
                           over_write_previous=True)

# feed csv file into address_base to convert into json files, insert_list will be a list of json filenames
insert_list = address_base.import_address_csv(input_file_location=address_base_csv_location,
                                              terminate_at=-1)

# indicate end of slicing
print("\nJSON conversion finished  %s\ncreated a total of %d files" %
      (datetime.now(), len(insert_list)))

# import each small file into mongodb database
number_files = insert_json_mongo(folder_name=address_json_folder,
                                 mongo_connection=mongo_destination)

print("\nImporting has finished at: %s" % datetime.now())