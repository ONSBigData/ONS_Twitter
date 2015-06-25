"""
Description:    Class definition for the Tweet, Address and AddressBase objects used in the importing process.
                Also contains functions for parsing invalid csv input row and latitude longitude conversion.
Author:         Bence Komarniczky
Date:           02/March/2015
Python version: 3.4
"""

from json import load, dump
from csv import reader
from datetime import datetime
from os.path import isfile

from bson.son import SON
from osgeo import ogr, osr
from pymongo.errors import OperationFailure

from ons_twitter.supporting_functions import distance
from ons_twitter.supporting_functions import create_folder


class Tweet(object):
    """
    Tweet class that contains a JSON object of tweet information.
    """

    def __init__(self, data=None, method=None):
        """
        Initialise class. Both data and method variables can be left empty for later completion of Tweet object.

        :param data:    Information to initialise the Tweet. Can be a list of variables (csv input) or a dictionary from
                        from Twitter API input.
        :param method:  Can hold 3 values: None, "csv", "json". None sets up empty Tweet object with NAs, csv expects
                        data to be a list, while json expects data to be dictionary from Twitter API/GNIP.
        :return:        Twitter object. Check .get_error() to see any errors.
        """
        self.error_number = 0
        self.error_description = []

        empty_dictionary = {'_id': "NA",
                            'user_id': "NA",
                            'unix_time': "NA",
                            'time': {
                                "timestamp": "NA",
                                "date": "NA",
                                "tod": "NA",
                                "dow": "NA",
                                "month": "NA"},
                            'tweet': {
                                "user_name": "NA",
                                "text": "NA",
                                "location": "NA",
                                "place": "NA",
                                "language": "NA",
                                "country": "NA",
                                "lat_long": ("NA", "NA"),
                                "coordinates": ("NA", "NA"),
                                "distance_from_centroid": "NA",
                                "address": {
                                    "UPRN": "NA",
                                    "coordinates": ["NA", "NA"],
                                    "postcode": "NA",
                                    "levels": {'oa11': "NA",
                                               'msoa11': "NA",
                                               'lsoa11': "NA",
                                               'oslaua': "NA",
                                               'osward': "NA",
                                               "wz11": "NA"},
                                    "classification": {
                                        "full": "NA",
                                        "abbreviated": "NA"},
                                    "distance": "NA"}}}

        if method is None:
            self.dictionary = empty_dictionary

        elif method == "csv":
            self.dictionary = empty_dictionary

            # check if last column is anything other than empty (generated by csv reader)
            # if not empty, then re-format the data using the parse_wrong_data function (there will be an extra comma)
            wrong_data_conversion = False
            if (len(data[-1]) > 0) and (len(data) > 10):
                data = parse_wrong_data(data, debug=False)
                wrong_data_conversion = True

            # modify csv structure here, expecting input of 11 columns
            try:
                self.dictionary["user_id"] = int(float(data[1]))
                self.dictionary["unix_time"] = int(float(data[0]))
                self.dictionary["tweet"]["user_name"] = data[2]
                self.dictionary["tweet"]["language"] = data[3]
                self.dictionary["tweet"]["location"] = data[4]
                self.dictionary["tweet"]["place"] = data[5]
                self.dictionary["tweet"]["country"] = data[6]
                self.dictionary["tweet"]["text"] = data[9].replace('"', "")
                self.dictionary["chunk_id"] = self.dictionary["user_id"] % 1000
                if wrong_data_conversion:
                    self.error_number = 2
            except ValueError:
                self.error_number = -1
                self.error_description.append("Invalid data supplied:    " + ",".join(data))
                if wrong_data_conversion:
                    # no geo found, even after row correction
                    self.error_number = 3
                else:
                    # no geo found and there were no problems with row
                    self.error_number = 1

            # handle missing coordinates
            try:
                self.dictionary["tweet"]["lat_long"] = (float(data[7]), float(data[8]))
                self.dictionary["tweet"]["coordinates"] = lat_long_to_osgb(
                    self.dictionary["tweet"]["lat_long"])
            except ValueError:
                self.dictionary["tweet"]["lat_long"] = ("NA", "NA")
                self.dictionary["tweet"]["coordinates"] = ("NA", "NA")
                self.error_number = 1
                self.error_description.append("Invalid lat_long coordinates supplied:    " + ",".join(data))

            # add time variables
            self.generate_time_input()
            self.dictionary["_id"] = str(self.dictionary["user_id"]) + "_" + str(self.dictionary["unix_time"])

        elif method == "json":
            self.dictionary = empty_dictionary

            # check if info is in dictionary, this indicates that this is only the end of file twitter API info
            if "info" in data.keys():
                self.error_number = 5
            else:

                # expect the current 2014-15 Twitter API json structure
                try:
                    self.dictionary["user_id"] = int(float(data["user"]["id"]))
                    self.dictionary["unix_time"] = int(float(data["timestamp_ms"])) // 1000
                    self.dictionary["tweet"]["user_name"] = data["user"]["name"]
                    self.dictionary["tweet"]["language"] = data["lang"]
                    self.dictionary["tweet"]["location"] = data["user"]["location"]
                    self.dictionary["tweet"]["place"] = data["place"]["name"]
                    self.dictionary["tweet"]["country"] = data["place"]["country_code"]
                    self.dictionary["tweet"]["text"] = data["text"].replace('"', "")
                    self.dictionary["chunk_id"] = self.dictionary["user_id"] % 1000
                except ValueError:
                    self.error_number = -1
                    self.error_description.append("Invalid data supplied:    " + ",".join(data))

                # handle missing coordinates
                try:
                    self.dictionary["tweet"]["lat_long"] = data["geo"]["coordinates"]
                    self.dictionary["tweet"]["coordinates"] = lat_long_to_osgb(
                        self.dictionary["tweet"]["lat_long"])
                except (ValueError, KeyError, TypeError):
                    self.dictionary["tweet"]["lat_long"] = ("NA", "NA")
                    self.dictionary["tweet"]["coordinates"] = ("NA", "NA")
                    self.error_number = 1
                    self.error_description.append("Invalid lat_long coordinates supplied:")
                    self.error_description.append(data)

                # add time variables
                self.generate_time_input()
                self.dictionary["_id"] = str(self.dictionary["user_id"]) + "_" + str(self.dictionary["unix_time"])

        else:
            print("Invalid method supplied to Tweet class!")

    def get_errors(self, print_status=False):
        """
        Method for checking error status of Tweet object.

        :param print_status:
        :return: Integer:   0 = "No errors",
                            1 = "no geo-location",
                            2 = "Wrong lat-long but conversion was successful",
                            3 = "Wrong lat-long and conversion was NOT successful",
                            5 = "End of file of GNIP output",
                            -1 = "Any other error"
        """

        if print_status:
            print(self.error_description)
        return self.error_number

    def get_info(self):
        """
        Method for pretty printing tweets.
        """

        from pprint import pprint

        pprint(self.dictionary)

    def generate_time_input(self):
        """"
        Method for converting raw unix_time input into different time variables.
        These new variables are then inserted into the tweet object. Used during initialisation.
        """

        # dependent input
        self.dictionary["_id"] = (self.dictionary["user_id"],
                                  self.dictionary["unix_time"])

        # time conversions | dependent input

        # timestamp
        self.dictionary["time"]["timestamp"] = datetime.fromtimestamp(
            self.dictionary["unix_time"]).strftime("%Y-%m-%d %X")
        # date
        self.dictionary["time"]["date"] = datetime.fromtimestamp(
            self.dictionary["unix_time"]).strftime("%Y-%m-%d")
        # month, abbreviated, e.g: Jan, Feb ...
        self.dictionary["time"]["month"] = datetime.fromtimestamp(
            self.dictionary["unix_time"]).strftime("%b")
        # Time of day hh:mm:ss
        self.dictionary["time"]["tod"] = datetime.fromtimestamp(
            self.dictionary["unix_time"]).strftime("%X")
        # Day of week Mon, Tue ...
        self.dictionary["time"]["dow"] = datetime.fromtimestamp(
            self.dictionary["unix_time"]).strftime("%a")

    def find_tweet_address(self, mongo_connection):
        """
        Finds the closest address point to the tweet from geo-indexed mongodb address base.
        :param mongo_connection: Geo-index mongodb collection.
        :return: 1 if address is found, 0 if no address is found within 300m of tweet. 2 for any other errors
        """
        # construct query
        query = {"coordinates": SON([("$near", self.dictionary["tweet"]["coordinates"]),
                                     ("$maxDistance", 300)])}
        # ask for single closest address if any
        try:
            closest_address_list = tuple(mongo_connection.find(query, {"_id": 0}).limit(1))
        except OperationFailure:
            print("Warning! Address base unavailable!")
            return 2

        # check if it has found any
        if len(closest_address_list) == 0:
            # if there are no address within 300m then add error description
            self.error_description.append("No address found within 300 meters")
            # add NA as distance
            self.dictionary["tweet"]["address"]["distance"] = "NA"
            return 1
        else:
            # add address
            self.dictionary["tweet"]["address"] = closest_address_list[0]
            # add distance (rounded to 3 decimal places
            self.dictionary["tweet"]["address"]["distance"] = distance(
                self.dictionary["tweet"]["coordinates"],
                closest_address_list[0]["coordinates"])
            return 0

    def add_cluster_info(self, cluster_data):
        pass

    def get_country_code(self):
        """
        Return the country code from the tweet. If non-GB then handle as special.
        """
        return self.dictionary["tweet"]["country"]

    def get_csv_format(self):
        """
        Return tweet as original csv row.
        """
        csv_row = [self.dictionary["unix_time"],
                   self.dictionary["user_id"],
                   self.dictionary["tweet"]["user_name"],
                   self.dictionary["tweet"]["language"],
                   self.dictionary["tweet"]["location"],
                   self.dictionary["tweet"]["place"],
                   self.dictionary["tweet"]["country"],
                   self.dictionary["tweet"]["lat_long"][0],
                   self.dictionary["tweet"]["lat_long"][1],
                   self.dictionary["tweet"]["text"]]
        return csv_row


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

    def __init__(self, folder_name, chunk_size=100000, over_write_previous=True):
        """
        Initialise AddressBase object.
        :param folder_name:     Folder to output JSON files to.
        :param chunk_size:      Number of addresses in each chunk of output.
                                Set this to -1 for one file.
        :param over_write_previous: False if files created need not be overwritten.
        :return:                AddressBase object.
        """
        create_folder(folder_name)
        self.collection = []
        self.folder_name = folder_name
        self.dump_index = 0
        self.chunk_size = chunk_size
        self.file_names = []
        self.over_write = over_write_previous

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
        # create new filename
        # leading zeroes are there to increase read performance
        file_name = self.folder_name + str(self.dump_index).zfill(8) + ".JSON"
        self.file_names.append(file_name)

        # only write if either file doesn't exist or overwrite is specified
        if self.over_write or not isfile(file_name):
            with open(file_name, 'w', newline="\n") as outfile:
                dump(self.collection, outfile, indent=2)

        # clear the collection and move onto next index
        self.collection.clear()
        print("Dumped index: ", self.dump_index, datetime.now())
        self.dump_index += 1

    def import_address_csv(self, input_file_location, header=True, terminate_at=-1):
        """
        Imports a csv file for insertion into address base.
        :param input_file_location: Location of address base file.
        :param header: Boolean if csv contains header row. Checking will be done in this case.
        :param terminate_at: Integer for debugging. Process terminates after this many rows.
        :return: a list of all output files
        """
        # add one to break point if header is true
        header_row = None
        if header:
            terminate_at += 1

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
            if len(self.collection) != 0:
                self.write_to_json()

        return self.file_names


def lat_long_to_osgb(lat_long):
    """
    Convert latitude, longitude coordinates to UK easting, northing coordinates.

    :param lat_long:    One single pair of latitude, longitude coordinates.
    :return:            A pair of easting (X), northing (Y) integer coordinates.

    :type lat_long      list or tuple
    :rtype              list
    """

    lat = lat_long[0]
    lng = lat_long[1]

    # Source is WSG84 (lat, lng) i.e. EPSG 4326:
    source = osr.SpatialReference()
    source.ImportFromEPSG(4326)

    # Target is osgb i.e. EPSG 27700:
    target = osr.SpatialReference()
    target.ImportFromEPSG(27700)

    # Prepare transformer
    transform = osr.CoordinateTransformation(source, target)

    # Create source point - coords are X, Y i.e. lng, lat:
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(lng, lat)

    # Now transform coordinates to target coord system:
    point.Transform(transform)

    # Return point as an (X, Y) tuple i.e. (easting, northing):
    return [int(point.GetX()), int(point.GetY())]


def parse_wrong_data(data, debug=False):
    """
    Used for parsing incorrect csv data formats into correct list objects.
    Some tweets contain "," in the place description or user name. If the quotes are incorrect then this can cause
    the raw input list to be too long. If the last object contains the tweet information and the list is longer than
    expected, then this function will find the extra element and remove it from the list.
    A new, cleaned data is returned.
    Main goal is to keep Longitude/Latitude information intact.

    :param data:    List resulting from csv reader - one row. User should check whether last element is non-empty. This
                    indicates invalid row.
    :param debug:   True for printing debug info.
    :return:        Cleaned data as as list of correct length (not guaranteed)

    :type data      list
    :type debug     bool
    :rtype          list
    """

    if debug:
        print("\nInput:", data)
    # country code should be in 6th place, everything after it should be computer generated
    # separate data into 3 objects - before/new/after. New_data will contain problematic sections of input
    country_index = data.index("GB")
    before_new_data = data[:2]
    new_data = data[2:(country_index - 1)]
    after_new_data = data[(country_index - 1):]
    # convert to string
    string_data = ",".join(new_data)

    # read json document of valid language codes
    language_codes_json = load(open("ons_twitter/twitter_lang_codes.JSON"))
    language_codes = []
    for one_item in language_codes_json:
        language_codes.append(one_item["code"])

    # loop over all language codes from Twitter
    for language in language_codes:
        lang_index = string_data.find(language)
        # if language code is found
        if lang_index != -1:
            if debug:
                print(language, lang_index)

            # separate at language code
            first_half = string_data[0:lang_index].split(sep=",")
            second_half = string_data[lang_index:].split(sep=",")

            # reduce both list to their correct length
            while len(first_half) > 1:
                first_half.pop()
            while len(second_half) > 2:
                second_half.pop()

            if debug:
                print(first_half)
                print(second_half)

            # paste new_data back together and finish loop
            new_data = first_half + second_half
            break

    # put data back together
    data = before_new_data + new_data + after_new_data

    # print debug info
    if debug:
        print("\n Final output:\n", data, "\n")

    # return the possibly! clean data list
    return data