#! Get Clusters
### load in data, assign data to
### geographic clusters by request type
### and store in database

### note that rtt is short for request type title and rtid is short for request type id

# rtt_dict={
#     "1966":"Trash & Recycling",
#     "124":"Street Lamp",
#     "373":"Signs / Bus Shelters / Pavement Markings",
#     "117":"Sidewalks and Curb damage",
#     "8516":"SNOW RELATED",
#     "116":"Potholes",
#     "2625":"Traffic/Road Safety",
#     "126":"Parks Request",
#     "51":"Traffic Signal / Pedestrian Signal",
#     "121":"Parking Violation/Abandoned Auto",
#     "3018":"Other - city responsibility",
#     "1251":"Private Property Issue",
#     "1249":"Public Space, Streets and Drains",
#     "374":"Other",
#     "1250":"Illegal Dumping",
#     "1853":"Tree Trimming",
#     "6215":"Hangers",
#     "2626":"Policing Issue",
#     "5185":"Health Complaints",
#     "122":"Graffiti",
#     "372":"Parking Meter",
#     "5743":"Bins for Trash & Recycling",
# }

from sklearn.cluster import KMeans
import pymongo
from pymongo import MongoClient
import numpy as np
from matplotlib import pyplot as plt
from firebase import firebase
import json
import pickle
import math
import datetime

def new_batch():
    client = MongoClient()
    db=client.scf_data
    if db.batches.find().count()>0:
        last_batch_id=db.batches.find().sort("id", pymongo.DESCENDING)[0]["id"]
        db.batches.insert_one({"id":last_batch_id+1,"created_at":datetime.datetime.now()})
    else:
        db.batches.insert_one({"id":1,"created_at":datetime.datetime.now()})


def get_clusters(city):
    ## connect to to the database and load issues
    client = MongoClient()
    db=client.scf_data

    current_batch_id = db.batches.find().sort("id", pymongo.DESCENDING)[0]["id"]
    if db.clusters.find().count()>0:
        current_cluster_id = db.clusters.find().sort("id", pymongo.DESCENDING)[0]["id"]
    else:
        current_cluster_id = 0

    lngs=[]
    lats=[]
    issue_ids=[]
    request_type_ids=[]
    for issue in db.issues.find({"city_id":city["id"]}):
        ## only create clusters for unresolved issues
        if issue["status"]=="Open" or issue["status"]=="Acknowledged":
            lats.append(issue["lat"])
            lngs.append(issue["lng"])
            issue_ids.append(issue["id"])
            request_type_ids.append(issue["request_type_id"])

    ## in order to approximate cartesian coordinates,
    ## scale the longitude values based on the city latitude
    scale_factor=math.cos((3.1415*city["lat"]/180.0))
    lngs_scaled=np.array(lngs)*scale_factor

    ## put lats and request_type_ids in a numpy array as well
    lats = np.array(lats)
    request_type_ids = np.array(request_type_ids)

    ## iterate through request_types
    ## and create a set of clusters for each
    ## use k_means, where k is the number of
    ## issues divided by 5.
    print "getting clusters"
    clusters = []
    clusters_issues=[]
    print len(request_type_ids)
    for request_type_id in list(set(request_type_ids)):
        print "....clustering "+ str(request_type_id)
        current_request_type_ind=(request_type_ids==request_type_id).nonzero()[0]

        if len(current_request_type_ind)>50:
            current_lngs = lngs_scaled[current_request_type_ind]
            current_lats = lats[current_request_type_ind]

            ## actual clustering happens here
            cluster = KMeans(len(current_request_type_ind)/5)
            cluster.fit(np.array([current_lngs,current_lats]).transpose())
            #cluster_centers = cluster.cluster_centers_
            cluster_labels = np.array(cluster.labels_)

            for cluster in list(set(cluster_labels)):
                ## map the cluster labels to the index of the issue ids
                current_cluster_ind=current_request_type_ind[cluster_labels==cluster]
                current_cluster_id+=1
                db.clusters.insert_one({
                    "id":current_cluster_id,
                    "batch_id":current_batch_id,
                    "request_type_id":request_type_id,
                    "city_id":city["id"],
                    "score":len(current_cluster_ind)})

                ## cluster issue relationships are stored separately
                ## to allow for multiple batches
                for current_issue_ind in current_cluster_ind:
                    db.clusters_issues.insert_one({
                        "issue_id":issue_ids[current_issue_ind],
                        "cluster_id":current_cluster_id})
