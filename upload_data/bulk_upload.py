import pymongo
from pymongo import MongoClient
import requests
import json
#
def bulk_upload(city):
    print "uploading data..."
    key=json.load(open('key_file.json','r'))["key"]
    base_url = "http://localhost:3000"

    #connect to mongodb server and create database...
    client = MongoClient()
    db= client.scf_data

    print "........uploading streets"
    ## upload streets, first get all issues into array
    streets_cursor = db.streets.find({"city_id":city["id_"]},{"_id":False},no_cursor_timeout=True)
    streets = [street for street in streets_cursor]
    streets_cursor.close()
    ## upload in batches of 1000
    for ind in range((len(streets)/1000)+1):
        startind=(1000*ind)
        stopind=min((1000*(ind+1)),len(streets))
        streets_to_upload=streets[startind:stopind]

        street_upload_url = base_url+"/api/bulk_upsert_street"
        street_upload_url += "?key="+key
        street_upload_params = {"streets":streets_to_upload}
        response=requests.post(
            street_upload_url,
            json=street_upload_params)

    print "........uploading issues"
    ## upload streets, first get all issues into array
    issues_cursor = db.issues.find({"city_id":city["id_"]},{"_id":False},no_cursor_timeout=True)
    issues = [issue for issue in issues_cursor]
    issues_cursor.close()
    ## upload in batches of 1000
    for ind in range((len(issues)/1000)+1):
        startind=(1000*ind)
        stopind=min((1000*(ind+1)),len(issues))
        issues_to_upload=issues[startind:stopind]

        issue_upload_url = base_url+"/api/bulk_upsert_issue"
        issue_upload_url += "?key="+key
        issue_upload_params = {"issues":issues_to_upload}
        response=requests.post(
            issue_upload_url,
            json=issue_upload_params)

    print "........uploading batch"
    ## upload latest batch
    latest_batch = db.batches.find({"city_id":city["id_"]},{"_id":False}).sort([("id_",pymongo.DESCENDING)])[0]

    print "........uploading clusters"
    ## upload clusters and clusters issues with latest batch id
    clusters_cursor = db.clusters.find({"batch_id":latest_batch["id_"]},{"_id":False})
    clusters = [cluster for cluster in clusters_cursor]
    cluster_ids = [cluster["id_"] for cluster in clusters]
    clusters_cursor.close()
    ## upload in batches of 1000
    for ind in range((len(clusters)/1000)+1):
        startind=(1000*ind)
        stopind=min((1000*(ind+1)),len(clusters))
        clusters_to_upload=clusters[startind:stopind]

        cluster_upload_url = base_url+"/api/bulk_upsert_cluster"
        cluster_upload_url += "?key="+key
        print clusters_to_upload
        cluster_upload_params = {"clusters":clusters_to_upload}
        response=requests.post(
            cluster_upload_url,
            json=cluster_upload_params)

    print "........uploading clusters issues"
    ## upload clusters and clusters issues with latest batch id
    clusters_issues_cursor = db.clusters_issues.find({"cluster_id":{"$in":cluster_ids}},{"_id":False})
    clusters_issues = [cluster_issue for cluster_issue in clusters_issues_cursor]
    clusters_issues_cursor.close()
    ## upload in batches of 1000
    for ind in range((len(clusters_issues)/1000)+1):
        startind=(1000*ind)
        stopind=min((1000*(ind+1)),len(clusters_issues))
        clusters_issues_to_upload=clusters_issues[startind:stopind]

        cluster_issue_upload_url = base_url+"/api/bulk_upsert_cluster_issue"
        cluster_issue_upload_url += "?key="+key
        cluster_issue_upload_params = {"clusters_issues":clusters_issues_to_upload}
        response=requests.post(
            cluster_issue_upload_url,
            json=cluster_issue_upload_params)

    ## upload batch once clusters are loaded
    batch_upload_url = base_url+"/api/create_batch"
    batch_upload_url += "?key="+key
    ##stringify latest_batch to convert to dict...
    latest_batch["created_at"]=str(latest_batch["created_at"])
    batch_upload_params = {"batch":latest_batch}
    response=requests.post(
        batch_upload_url,
        json=batch_upload_params)

    print "........uploading request types"
    ## upload request types
    for request_type in db.request_types.find({"city_id":city["id_"]},{"_id":False}):
        request_type_upload_url = base_url+"/api/create_request_type"
        request_type_upload_url += "?key="+key
        request_type_upload_params = {"request_type":request_type}
        response=requests.post(
            request_type_upload_url,
            json=request_type_upload_params)

    print "........uploading city"
    ## upload cities
    city = db.cities.find_one({"id_":city["id_"]},{"_id":False})
    city_upload_url = base_url+"/api/create_city"
    city_upload_url += "?key="+key
    city_upload_params = {"city":city}
    response=requests.post(
        city_upload_url,
        json=city_upload_params)
