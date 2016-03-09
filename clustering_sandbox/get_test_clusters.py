#######
#######
#######
#congfig...

city_id = 3039 # New Haven, CT is the default
base_filename = "clusters" ## "clusters" is the default
## import the clustering algorithm that we want to use...
from clustering_algorithms import k_means_clustering as cluster_issues## kmeans is the default

#######
#######
#######

import json ## use this for file io

## setup pymongo to interact with database
## where data is stored
import pymongo
from pymongo import MongoClient
client = MongoClient()
db = client.scf_data

## pull city data from the database..
city = db.cities.find_one({"id":city_id})

## iterative over request types, getting
## issue coordinates for all open issues in each
for request_type in db.request_types.find({"city_id":city_id}):
    request_type_id = request_type["id"]

    lngs=[]
    lats=[]
    issue_ids =[]
    for issue in db.issues.find(
        {"request_type_id":request_type_id,
        "status":{"$in":["Open","Acknowledged"]}}):

        lngs.append(issue["lng"])
        lats.append(issue["lat"])
        issue_ids.append(issue["id"])
    ## check that there are a reasonable number of
    ## issues in the collection, say 50
    if len(lngs)>50:
        ## get clusters
        clusters_ind = cluster_issues(lngs,lats,city["lng"],city["lat"])

        ## translate array of indices into array of issue ids
        clusters = []
        for cluster_ind in clusters_ind:
            cluster=[]
            for ind in cluster_ind:
                cluster.append({
                    "issue_id":issue_ids[ind],
                    "lat":lats[ind],
                    "lng":lngs[ind]})
            clusters.append(cluster)
        ## write cluster data to a json file
        output_filepath = "cluster_data/"+base_filename
        output_filepath += "_rtid-"+str(request_type_id)
        output_filepath += "_cityid-"+str(city["id"])
        with open(output_filepath,"w") as f:
            json.dump(clusters,f)
    else:
        print "request type #",request_type_id,"had less than 50 issues"
