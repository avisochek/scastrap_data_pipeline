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

def delete_old_clusters(batch):
    client = MongoClient()
    db=client.scf_data
    ## get rid of old clusters_issues
    for cluster in db.clusters.find({
        "city_id":batch["city_id"],
        "batch_id":{"$ne":batch["id_"]}}):
        db.clusters_issues.remove({
            "cluster_id":cluster["id_"]})
    ## get rid of old clusters
    db.clusters.remove({
        "city_id":batch["city_id"],
        "batch_id":{"$ne":batch["id_"]}})
    ## get rid of old batches
    db.batches.remove({
        "city_id":batch["city_id"],
        "id_":{"$ne":batch["id_"]}})

def new_batch(city):
    client = MongoClient()
    db=client.scf_data
    if db.batches.find().count()>0:
        last_batch_id=db.batches.find().sort("id_", pymongo.DESCENDING)[0]["id_"]
    else:
        last_batch_id=0
    batch={
        "id_":last_batch_id+1,
        "created_at":datetime.datetime.now(),
        "city_id":city["id_"]}
    db.batches.insert_one(batch)
    delete_old_clusters(batch)
    return batch

def get_clusters(city):
    ## create a new batch for current city_id
    current_batch=new_batch(city)
    current_batch_id = current_batch["id_"]

    cluster_diameter=2000
    ## connect to to the database and load issues
    client = MongoClient()
    db=client.scf_data

    ## get latest cluster id
    if db.clusters.find().count()>0:
        current_cluster_id = db.clusters.find().sort("id_", pymongo.DESCENDING)[0]["id_"]
    else:
        current_cluster_id = 0

    ## get request_type_ids
    request_type_ids=[request_type["id_"] for request_type in db.request_types.find({"city_id":city["id_"]})]


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
            "city_id":city["id_"],
            "request_type_id":request_type_id,
            "status":{"$in":["Open","Acknowledged"]}}):
            lngs.append(issue["lng"])
            lats.append(issue["lat"])
            issue_ids.append(issue["id_"])

        ## actual clustering happens here using mcl
        clusters_ind=mcl(lngs,lats,city,cluster_diameter)

        ## make a single cluster for each issue not covered...
        used_ind=[]
        for cluster_ind in clusters_ind:
            used_ind+=cluster_ind
        unused_ind = [ind for ind in range(len(issue_ids)) if ind not in used_ind]
        for ind in unused_ind:
            clusters_ind.append([ind])

        ## insert clusters and cluster issue relations in database
        for cluster_ind in clusters_ind:
            cluster_lng = np.array(lngs)[cluster_ind]
            cluster_lat = np.array(lats)[cluster_ind]
            used_ind+=cluster_ind
            ## map the cluster labels to the index of the issue ids
            current_cluster_id+=1
            db.clusters.insert_one({
                "id_":current_cluster_id,
                "batch_id":current_batch_id,
                "request_type_id":request_type_id,
                "city_id":city["id_"],
                "score":len(cluster_ind),
                "lng":(min(cluster_lng)+max(cluster_lng))/2,
                "lat":(min(cluster_lat)+max(cluster_lat))/2})

            ## cluster issue relationships are stored separately
            ## to allow for multiple batches
            for issue_ind in cluster_ind:
                db.clusters_issues.insert_one({
                    "issue_id":issue_ids[issue_ind],
                    "cluster_id":current_cluster_id})
