#! Get Clusters
### load in data, assign data to
### geographic clusters by request type
### and store in database

from sklearn.cluster import KMeans
import pymongo
from pymongo import MongoClient
import numpy as np
import datetime
from qwer import mcl

def new_batch(city):
    client = MongoClient()
    db=client.scf_data
    if db.batches.find().count()>0:
        last_batch_id=db.batches.find().sort("id", pymongo.DESCENDING)[0]["id"]
        db.batches.insert_one({
            "id":last_batch_id+1,
            "created_at":datetime.datetime.now(),
            "city_id":city["id"]})
    else:
        db.batches.insert_one({
            "id":1,
            "created_at":datetime.datetime.now(),
            "city_id":city["id"]})


def get_clusters(city):
    ## create a new batch for current city_id
    new_batch(city)

    cluster_diameter=2000
    ## connect to to the database and load issues
    client = MongoClient()
    db=client.scf_data

    ## get latest batch id for current city
    current_batch_id = db.batches.find({"city_id":city["id"]}).sort("id", pymongo.DESCENDING)[0]["id"]
    ## get latest cluster id
    if db.clusters.find().count()>0:
        current_cluster_id = db.clusters.find().sort("id", pymongo.DESCENDING)[0]["id"]
    else:
        current_cluster_id = 0

    ## get request_type_ids
    request_type_ids=[request_type["id"] for request_type in db.request_types.find({"city_id":city["id"]})]


    ## iterate through request_types
    ## and create a set of clusters for each
    ## use markov cluster
    print "getting clusters"
    clusters = []
    clusters_issues=[]
    print len(request_type_ids)
    for request_type_id in list(set(request_type_ids)):
        print "....clustering "+ str(request_type_id)
        lngs,lats,issue_ids=[],[],[]
        for issue in db.issues.find({
            "city_id":city["id"],
            "request_type_id":request_type_id,
            "status":{"$in":["Open","Acknowledged"]}}):
            lngs.append(issue["lng"])
            lats.append(issue["lat"])
            issue_ids.append(issue["id"])

        ## actual clustering happens here using mcl
        clusters_ind=mcl(lngs,lats,city,cluster_diameter)
        for cluster_ind in clusters_ind:
            ## map the cluster labels to the index of the issue ids
            current_cluster_id+=1
            db.clusters.insert_one({
                "id":current_cluster_id,
                "batch_id":current_batch_id,
                "request_type_id":request_type_id,
                "city_id":city["id"],
                "score":len(cluster_ind)})

            ## cluster issue relationships are stored separately
            ## to allow for multiple batches
            for issue_ind in cluster_ind:
                db.clusters_issues.insert_one({
                    "issue_id":issue_ids[issue_ind],
                    "cluster_id":current_cluster_id})
