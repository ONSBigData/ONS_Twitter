__author__ = 'Bence Komarniczky'
"""
Code to run DBScan clustering on tweets collected between April and August.
This code uses the joblib module and requires the tweets to be hashed before
processing by userid.
Date: 30 October 2014
Python version: 3.4.1
"""


from joblib import Parallel, delayed
import datetime
import math
import gc
import csv
import os

gc.collect()
# Set number of cores
gc.enable()

"""Variables to modify"""
# number of cpu cores to use
num_cores = 2

# input folder, containing csv files hashed by user tweets
input_folder = "data/input/"

# output folder, where folders for csv files will be generated and populated
output_folder = "data/output/clustered/"

# distance parameter for DBScan
distance_eps = 20

# robot search, pre-clustering on/off
do_robot = True

# number of csv files to cluster, set this to a low number to test
how_many_to_do = 1000

"""End of user specific variables"""

print(datetime.datetime.now().strftime('%d-%b-%y %H:%M:%S'))

""" a list of know robot twitter users.
These will be completely ignored by the clustering algorithm and they won't appear in the final files."""
robot_list = {497145453, 35201757, 1544159024, 1353095870, 606204776, 281177385,
              19711240, 1484740038, 2336599820, 2586094238, 37402072, 228300826, 2484565742,
              473177084, 630953012, 963227118, 174587664, 819222966, 1266803563}


def create_list_user(input_file):
    """This function reads in a single csv file and creates a Python dictionary with keys corresping to user
    numbers and the values corresponding to tweet information."""

    global robot_list
    list_user_tweets = dict()

    with open(input_file, newline="\n", encoding='utf-8') as csvfile:
        dataframe = csv.reader(csvfile, delimiter=',')
        for line in dataframe:

            userid = int(float(line[2]))

            if userid in robot_list:
                continue
                # create an empty list for the userid

            # round timestamp
            line[0] = int(round(float(line[0])))
            # create floats of northings and eastings

            line[6] = float(line[6])
            line[7] = float(line[7])

            try:
                list_user_tweets[userid].append(line)
            except KeyError:
                list_user_tweets[userid] = [line]

    return list_user_tweets


def cluster(userid, twitter_dictionary, eps=distance_eps, min_points=3, robot_search=False):
    # read in part of the list to be clustered, only one user!!!
    reader = twitter_dictionary[userid]
    reader_l1 = reader[:]
    full_info = []

    # Set up variables for cluster
    unique_clusters = []
    cluster_count = 1
    cluster_loc_list = set([])

    # robot search
    if robot_search and len(reader) > 1000:
        # print("User", reader[0][2], "has", len(reader), "tweets")
        #put them into buckets
        #then count the number of elements in the buckets
        buckets = {}
        for tweet in reader:
            (timestamp_a, date, userid, bmg, lat, long, northing_a, easting_a) = tweet
            if not ((northing_a // 2) * 2, (easting_a // 2) * 2) in buckets.keys():
                buckets[((northing_a // 2) * 2, (easting_a // 2) * 2)] = [tweet]
            else:
                buckets[((northing_a // 2) * 2, (easting_a // 2) * 2)].append(tweet)
        for key_index in buckets.keys():
            if len(buckets[key_index]) > (0.2 * len(reader)):
                cluster_expand = []
                pt_sum = [0, 0]

                pt_count = len(buckets[key_index])
                for tweet in buckets[key_index]:
                    (timestamp_a, date, userid, bmg, lat, long, northing_a, easting_a) = tweet
                    cluster_loc_list.add(timestamp_a)
                    dow = datetime.datetime.fromtimestamp(float(timestamp_a)).strftime('%a')
                    tod = datetime.datetime.fromtimestamp(float(timestamp_a)).strftime('%H:%M:%S')
                    cluster_expand.append(
                        [timestamp_a, date, userid, lat, long, bmg, northing_a, easting_a, dow, tod])
                    pt_sum[0] += northing_a
                    pt_sum[1] += easting_a
                    reader_l1.remove(tweet)
                cluster_type = "ROBOT"
                #create cluster id

                cluster_id = "%s_%s%sROBOT" % (userid, bmg, cluster_count)
                centroid = [math.ceil(float(pt_sum[0]) / pt_count), math.ceil(float(pt_sum[1]) / pt_count)]
                max_distance = 0.0
                for one_tweet in cluster_expand:
                    centroid_distance = (((centroid[0] - one_tweet[6]) ** 2) + (
                        centroid[1] - one_tweet[7]) ** 2) ** .5
                    #update distance for furthest point
                    max_distance = max(centroid_distance, max_distance)
                    #userid, #unix time,  #date # place #northing # easting #day #time
                    #  cluster_id # centroid_n #centroid_e #distance
                    new_row = u"{0:s},{1:d},{2:s},{3:s},{4:d},{5:d},{6:s},{7:s},{8:s},{9:d},{10:d},{11:.0f},{12:d}".format(
                        one_tweet[2], one_tweet[0], one_tweet[1], one_tweet[3],
                        round(one_tweet[6]),
                        round(one_tweet[7]),
                        one_tweet[8],
                        one_tweet[9],
                        cluster_id,
                        centroid[0],
                        centroid[1],
                        centroid_distance,
                        pt_count)

                    #append tweet info to full_info
                    full_info.append(new_row)
                #print new_row
                #save unique clusters

                new_cluster = u"{0:s},{1:s},{2:d},{3:d},{4:d},{5:s},{6:.2f},0,0".format(userid, cluster_id, centroid[0],
                                                                                        centroid[1], pt_count,
                                                                                        cluster_type, max_distance)

                cluster_count += 1
                #append new_cluster to unique clusers
                unique_clusters.append(new_cluster)

                #create search lists
    reader_l2 = reader_l1[:]
    reader_l3 = reader_l1[:]

    # iterate over tweets for user
    for row_a in reader_l1:

        (timestamp_a, date, userid, bmg, lat, long, northing_a, easting_a) = row_a

        #set pt_count to 1, a is the first point in the cluster
        pt_count = 1

        #create unique cluster_id
        cc = str(cluster_count)
        cluster_id = "%s_%s%s" % (userid, bmg, cc)

        #debug
        # print("new reader_l1:")
        # for a in reader_l1:
        #     print(a)

        #Test whether point is already in cluster, using timestamp
        if not timestamp_a in cluster_loc_list:

            #write info of row in cluster_expand

            #calculate Day of Week and Time of Day
            dow = datetime.datetime.fromtimestamp(float(timestamp_a)).strftime('%a')
            tod = datetime.datetime.fromtimestamp(float(timestamp_a)).strftime('%H:%M:%S')

            cluster_expand = [[timestamp_a, date, userid, bmg, lat, long, northing_a, easting_a, dow, tod]]

            #count the number of points in cluster and their location
            cluster_count += 1
            cluster_pts = [row_a]

            #add point a to cluster_loc_list
            cluster_loc_list.add(timestamp_a)

            #keep track of centroid
            pt_sum = [northing_a, easting_a]

            #resize search lists

            reader_l2.remove(row_a)
            if row_a in reader_l3:
                reader_l3.remove(row_a)

            # ------------ START OF POINT B -------------------
            for row_b in reader_l2:

                (timestamp_b, date, userid, bmg, lat, long, northing_b, easting_b) = row_b
                #modify this line to increase col count

                #don't compare row with itself
                if timestamp_b in cluster_loc_list:
                    pass

                else:
                    #convert easting and northing to floats and check for NAs

                    #Calculate euclidean distance between a-b
                    dist = (((northing_a - northing_b) ** 2) + (easting_a - easting_b) ** 2) ** 0.5

                    #add point b to cluster if it is within eps distance of a
                    if dist < eps:
                        #increase point count
                        pt_count += 1

                        #add point coordinates + info
                        cluster_pts.append(row_b)

                        #calculate DayofWeek and TimeofDay
                        dow = datetime.datetime.fromtimestamp(float(timestamp_b)).strftime('%a')
                        tod = datetime.datetime.fromtimestamp(float(timestamp_b)).strftime('%H:%M:%S')

                        cluster_expand.append(
                            [timestamp_b, date, userid, bmg, lat, long, northing_b, easting_b, dow, tod])
                        cluster_loc_list.add(timestamp_b)

                        #keep track of centroid
                        pt_sum[0] += northing_b
                        pt_sum[1] += easting_b

                        #update search list
                        reader_l3.remove(row_b)

            # ----------------- END OF POINT B ------------------
            #check other points if b has been found
            if pt_count != 1:

                #debug                print "b has been found"

                #iterate through points already found in cluster
                for a in cluster_pts:
                    northing_c = a[6]
                    easting_c = a[7]

                    if a in reader_l3:
                        reader_l3.remove(a)

                    # ----------- START OF POINT D -----------------
                    #then iterate for each individual point outside the cluster
                    for row_d in reader_l3:

                        (timestamp_d, date, userid, bmg, lat, long, northing_d, easting_d) = row_d
                        #modify this line to increase col count

                        #check if point has already been clustered:
                        if not (timestamp_d in cluster_loc_list):

                            #calculate distance between c and d
                            dist = (((northing_d - northing_c) ** 2) + (easting_d - easting_c) ** 2) ** 0.5

                            if dist < eps:
                                pt_count += 1
                                cluster_pts.append(row_d)
                                cluster_loc_list.add(timestamp_d)

                                #calculate DayofWeek and TimeofDay
                                dow = datetime.datetime.fromtimestamp(float(timestamp_d)).strftime('%a')
                                tod = datetime.datetime.fromtimestamp(float(timestamp_d)).strftime('%H:%M:%S')

                                #add point's info to cluster list
                                cluster_expand.append(
                                    [timestamp_d, date, userid, bmg, lat, long, northing_d, easting_d, dow, tod])

                                #keep track of centroid
                                pt_sum[0] += northing_d
                                pt_sum[1] += easting_d
                                # ------------- END OF POINT D ------------------
            #calculate centroid location
            centroid = [math.ceil(float(pt_sum[0]) / pt_count), math.ceil(float(pt_sum[1]) / pt_count)]

            #flag cluster as either noise or cluster
            if pt_count < min_points:
                cluster_type = "noise"
            else:
                cluster_type = "cluster"

            variance = 0.0
            max_distance = 0.0
            mean = 0.0
            #save all tweets with cluster id
            for one_tweet in cluster_expand:
                centroid_distance = (((centroid[0] - one_tweet[6]) ** 2) + (centroid[1] - one_tweet[7]) ** 2) ** 0.5

                variance += (centroid_distance ** 2)
                mean += centroid_distance
                #update distance for furthest point
                max_distance = max(centroid_distance, max_distance)

                #           #userid, #unix time,  #date # place #northing # easting #day #time
                #  cluster_id # centroid_n #centroid_e #distance

                new_row = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (
                    one_tweet[2], one_tweet[0], one_tweet[1], one_tweet[3],
                    int(round(one_tweet[6])),
                    int(round(one_tweet[7])),
                    one_tweet[8],
                    one_tweet[9],
                    cluster_id,
                    centroid[0],
                    centroid[1],
                    int(centroid_distance),
                    pt_count)

                #append tweet info to full_info
                full_info.append(new_row)

            #save unique clusters
            mean = mean / pt_count
            std = (variance / max(pt_count - 1, 1)) ** .5

            new_cluster = u"{0:s},{1:s},{2:d},{3:d},{4:d},{5:s},{6:.2f},{7:.4f},{8:.4f}".format(userid, cluster_id,
                                                                                                centroid[0],
                                                                                                centroid[1],
                                                                                                pt_count, cluster_type,
                                                                                                max_distance, std,
                                                                                                mean)

            #append new_cluster to unique clusers
            unique_clusters.append(new_cluster)

    return unique_clusters, full_info


def parallel_clustering(tweets, user_no=0, num_cores=8, robot_search=False):
    """Clusters a dictionary into a list,
    where each element is a collection of clusters for that user"""
    print(datetime.datetime.now().strftime('%d-%b-%y %H:%M:%S'))
    if user_no == 0:
        inputs = tweets.keys()
    else:
        inputs = tweets.keys()[0:(user_no - 1)]
    results = Parallel(n_jobs=num_cores)(delayed(cluster)(i, tweets, robot_search=robot_search) for i in inputs)
    print("Job finished at:", datetime.datetime.now().strftime('%d-%b-%y %H:%M:%S'))
    return results


def cluster_this(tweets, user_no=0, num_cores=8, robot_search=False):
    """Function for running a clutering job and then combining the lists into one big list"""
    app_begin = datetime.datetime.now()

    results = parallel_clustering(user_no=user_no, tweets=tweets, num_cores=8,
                                  robot_search=robot_search)
    parallel_end = datetime.datetime.now()

    print("Begin combining lists", datetime.datetime.now().strftime('%d-%b-%y %H:%M:%S'))

    new_list = []
    full_list = []
    for users in results:
        for types in [0, 1]:

            if types == 0:
                new_list += users[types]
            else:
                full_list += users[types]

    print("Parallel process took:", parallel_end - app_begin)

    return [new_list, full_list]


def parallel_csv(csvname, output_folder, input_folder):
    # create dictionary from csvfile
    start_time = str(datetime.datetime.now().strftime('%d-%b-%y %H:%M:%S'))
    filename = input_folder + csvname
    twitter_dictionary = create_list_user(filename)

    # set up final lists
    unique_clusters = []
    full_info = []

    # start clustering
    for user_name in twitter_dictionary.keys():
        # create list with two elements, unique_clusters and full_list
        out_user = cluster(user_name, twitter_dictionary, robot_search=do_robot)

        #collect info to final list

        unique_clusters.append(out_user[0])
        full_info.append(out_user[1])

    # write final_list to csvfiles

    outfolder = output_folder + csvname.split(".")[0] + "/"
    output_file_clusters = outfolder + "unique_clusters.csv"
    output_file_tweets = outfolder + "clustered_tweets.csv"

    #print("out_clusters:", output_file_clusters)

    with open(output_file_clusters, 'w', newline="\n") as out_file:
        outcsv = csv.writer(out_file, doublequote=False, quoting=csv.QUOTE_MINIMAL)
        for users in unique_clusters:
            for row in users:
                row = row.split(",")
                outcsv.writerow(row)

    with open(output_file_tweets, 'w', newline="\n") as out_file:
        outcsv = csv.writer(out_file, doublequote=False, quoting=csv.QUOTE_MINIMAL)
        for users in full_info:
            for row in users:
                row = str(row).split(",")
                outcsv.writerow(row)
    end_time = str(datetime.datetime.now().strftime('%d-%b-%y %H:%M:%S'))
    with open(outfolder + "debug.txt", 'w', newline="\n") as outfile:
        myfile = csv.writer(outfile, escapechar=" ", quoting=csv.QUOTE_NONE)
        debug = ["start time: ", start_time, " job ended at: ", end_time]
        myfile.writerow(debug)
    print("Done:" + csvname)


def do_clustering(number_of_csv, input_folder=input_folder,
                  output_folder=output_folder, num_cores=8):
    name_list = [(str(i) + ".csv") for i in range(number_of_csv)]
    if __name__ == "__main__":
        Parallel(n_jobs=num_cores)(delayed(parallel_csv)(csvname, input_folder=input_folder,
                                                         output_folder=output_folder)
                                   for csvname in name_list)
        print("Job done!")
        print(str(datetime.datetime.now().strftime('%d-%b-%y %H:%M:%S')))


# create output folders

for i in range(how_many_to_do):
    new_path = output_folder + str(i)
    if not os.path.exists(new_path):
        os.makedirs(new_path)

# run clustering algorithm

do_clustering(how_many_to_do, num_cores=num_cores)
