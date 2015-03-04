"""
Description:    Functions to handle UK address base and its import into MongoDB
Author:         Bence Komarniczky
Date:           04/March/2015
Python version: 3.4
"""

from csv import reader
from bson.son import SON
from json import dump


class Address():
    """
    Object of type Address corresponds to a singe data point of the UK address base.
    Most important part is a JSON object with all the info from the original csv file.
    """
    def __init__(self, data, header_row=None):
        """
        Supply a list or tuple of data points and a corresponding header file to insert data
        into object.
        :param data:        row of csv file with observation
        :param header_row:  header row of original csv file - used for checking correctness of input
        :return:            object of type address
        """

        # initialise error codes and empty dictionary
        self.error_code = 0
        empty_dictionary = {"UPRN": "NA",
                            "coordinates": ("NA", "NA"),
                            "postcode": "NA",
                            "classification": {
                                "full": "NA",
                                "abbreviated": "NA"},
                            "levels": {'oa11': "NA",
                                       'msoa11': "NA",
                                       'lsoa11': "NA",
                                       'osward': "NA",
                                       'oslaua': "NA",
                                       "wz11": "NA"}}

        # if header row is supplied, then check against original
        if header_row is not None:
            # headers from original file
            pre_header = ['POSTCODE', 'UPRN', 'X_COORDINATE',
                          'Y_COORDINATE', 'CLASSIFICATION_CODE',
                          'oslaua', 'osward', 'oa11', 'lsoa11',
                          'msoa11', 'wz11']
            if pre_header == header_row:
                # header row matches, continue reading
                self.error_code = 0
            else:
                # error in header row
                print("Header row is different from original, terminating...")
                self.dictionary = None
                self.error_code = 1

        if self.error_code == 0:
            # start populating dictionary
            self.dictionary = empty_dictionary
            self.dictionary["postcode"] = data[0]
            self.dictionary["UPRN"] = int(float(data[1]))
            self.dictionary["coordinates"] = (int(float(data[2])), int(float(data[3])))
            self.dictionary["classification"]["full"] = data[4]
            self.dictionary["classification"]["abbreviated"] = data[4][0]

            # take care of missing levels
            level_tuple = ("oslaua", "osward", "oa11", "lsoa11", "msoa11", "wz11")
            index = 5
            for level_name in level_tuple:
                if len(data[index]) == 0:
                    index += 1
                    continue
                else:
                    self.dictionary["levels"][level_name] = data[index]
                    index += 1

    def get_bson(self):
        """
        Return a bson object of the dictionary held within the address.
        This is to increase compatibility with mongodb
        """
        if self.dictionary is not None:
            return SON(self.dictionary)
        else:
            return -1


class AddressBase():
    """
    Object with collection of addresses that automatically dumps json files as populated.
    """

    def __init__(self, folder_name, chunk_size):
        """
        Initialise AddressBase object.
        :param folder_name:     Folder to output JSON files to.
        :param chunk_size:      Number of addresses in each chunk of output.
                                Set this to -1 for one file.
        :return:                AddressBase object.
        """
        self.collection = []
        self.folder_name = folder_name
        self.dump_index = 0
        self.chunk_size = chunk_size
        self.file_names = []

    def add_address(self, new_address):
        """
        Adds an Address type object to the AddressBase
        :param new_address: object of type Address for insertion.
        :return:    none
        """
        self.collection.append(new_address.get_bson())
        # as it reaches chunk size limit, dump it into csv
        if len(self.collection) == self.chunk_size:
            self.write_to_json()

    def write_to_json(self):
        """
        Function to write addresses to a single JSON file.
        :return: none
        """
        file_name = self.folder_name + str(self.dump_index) + ".JSON"
        self.file_names.append(file_name)
        with open(file_name, 'w') as outfile:
            dump(self.collection, outfile)
        self.collection.clear()
        self.dump_index += 1

    def import_address_csv(self, input_file_location, header=True, terminate_at=-1):
        """
        Imports a csv file for insertion into address base.
        :param input_file_location: Location of address base file.
        :param header: Boolean if csv contains header row. Checking will be done in this case.
        :param terminate_at: Integer for debugging. Process terminates after this many rows.
        :return: a list of all output files
        """
        with open(input_file_location, 'r', newline="\n") as open_csv:
            address_csv = reader(open_csv)
            index = 0

            # do we have headers?
            if header:
                for row in address_csv:
                    index += 1
                    # pick up header row
                    if index == 1:
                        header_row = row
                        continue

                    # create new address, with checking
                    new_address = Address(row, header_row=header_row)
                    # add new address
                    self.add_address(new_address)

                    if index == terminate_at:
                        break
            else:
                for row in address_csv:
                    # increase index
                    index += 1
                    # create new address
                    new_address = Address(row, header_row=None)
                    # add new address
                    self.add_address(new_address)

                    if index == terminate_at:
                        break

            # write any remaining files
            self.write_to_json()

        return self.file_names