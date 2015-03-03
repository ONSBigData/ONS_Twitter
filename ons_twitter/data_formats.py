"""
Description:    Supplementary Python file, containing file formats for Twitter -> MongoDB imports/exports.
Author:         Bence Komarniczky
Date:           02/March/2015
Python version: 3.4
"""

from datetime import datetime


class Tweet():
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

        empty_dictionary = {'_id': [],
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
                                "long_lat": ["NA", "NA"],
                                "coordinates": ["NA", "NA"],
                                "distance_from_centroid": "NA",
                                "address": {
                                    "UPRN": "NA",
                                    "coordinates": ["NA", "NA"],
                                    "postcode": "NA",
                                    "levels": {'oa11': "NA",
                                               'msoa11': "NA",
                                               'lsoa11': "NA",
                                               'oslaua': "NA",
                                               'osward': "NA"},
                                    "classification": {
                                        "full": "NA",
                                        "abbreviated": "NA"},
                                    "distance": "NA"}}}

        if method is None:
            self.dictionary = empty_dictionary

        elif method == "csv":
            self.dictionary = empty_dictionary
            # modify csv structure here
            self.dictionary["user_id"] = int(float(data[1]))
            self.dictionary["unix_time"] = int(float(data[0]))
            self.dictionary["tweet"]["user_name"] = data[2]

        elif method == "json":
            pass
        else:
            print("Invalid method supplied to Tweet class!")

    def get_errors(self, print_status=False):
        """
        Method for checking error status of Tweet object.

        :param print_status:
        :return: Integer: 0 = "No errors", 1 = "no geo-location", 2 = "Any other error"
        """

        if print_status:
            print(self.error_discription)
        return self.error_number

a = utm.from_latlon(54.95831761, -1.73138835)
print(a)