#######
#######
#######
#congfig... set parameters below

city_id = 3039 # New Haven, CT is the default
base_filename = "mcl1000"
## import the clustering algorithm that we want to use...
from clustering_algorithms import mcl as cluster_issues## kmeans is the default
## set minimum number of issues for each set of clusters
min_issues=1
##set the cluster mathdiameter in feet
cluster_diameter=1000
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
city = db.cities.find_one({"id_":city_id})

## iterative over request types, getting
## issue coordinates for all open issues in each
for request_type in db.request_types.find({"city_id":city_id}):
    request_type_id = request_type["id_"]
    lngs=[]
    lats=[]
    issue_ids =[]
    for issue in db.issues.find(
        {"request_type_id":request_type_id,
        "status":{"$in":["Open","Acknowledged"]}}):

        lngs.append(issue["lng"])
        lats.append(issue["lat"])
        issue_ids.append(issue["id_"])
    ## check that there are a reasonable number of
    ## issues in the collection, say 50
    if len(lngs)>min_issues:
        ## get clusters
        clusters_ind = cluster_issues(lngs,lats,city,cluster_diameter)
        ## the last cluster should represent
        ## all of the issues that aren't used
        used_issues_ind=[]
        for cluster_ind in clusters_ind:
            used_issues_ind+=cluster_ind
        unused_issues_ind=[ind for ind in range(len(issue_ids)) if not ind in used_issues_ind]
        #print request_type_id,len(used_issues_ind+unused_issues_ind)
        clusters_ind.append(unused_issues_ind)

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
        output_filepath += "_cityid-"+str(city["id_"])
        with open(output_filepath,"w") as f:
            json.dump(clusters,f)
    else:
        continue
        #print "request type #",request_type_id,"had less than 50 issues"
