"""
Description:    Supplementary Python file, containing file formats for Twitter -> MongoDB imports/exports.
Author:         Bence Komarniczky
Date:           02/March/2015
Python version: 3.4
"""


class Tweet(data=None, method=None):
    """
    Tweet class that contains a JSON object of tweet information.
    """

    def __init__(self, data, method):
        """
        Initialise class. Both data and method variables can be left empty for later completion of Tweet object.

        :param data:    Information to initialise the Tweet. Can be a list of variables (csv input) or a dictionary from
                        from Twitter API input.
        :param method:  Can hold 3 values: None, "csv", "json". None sets up empty Tweet object with NAs, csv expects
                        data to be a list, while json expects data to be dictionary from Twitter API/GNIP.
        :return:        Twitter object. Check .get_error() to see any errors.
        """

        self.empty_dictionary = {'_id': [], 'user_id': "NA", 'unix_time': "NA", 'time': {
            "timestamp": "NA",
            "date": "NA",
            "tod": "NA",
            "dow": "NA",
            "month": "NA"}, 'tweet': {
            "location": "NA",
            "place": "NA",
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
                    "abbreviated": "NA"
                },
                "distance": "NA"
            }}}

        if method is None:
            self.dictionary = self.empty_dictionary

        elif method == "csv":
            pass
        elif method == "json":
            pass
        else:
            print("Invalid method supplied to Tweet class!")

    def get_errors(self, print_status= False):
        """
        Method for checking error status of Tweet object.

        :param print_status:
        :return: Integer: 0 = "No errors", 1 = "no geo-location", 2 = "Any other error"
        """

        if print_status:
            print(self.error_discription)
        return self.error_number
