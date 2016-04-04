import pymongo
from pymongo import MongoClient
import requests
import json
#
def upload_data(city):
    print "uploading data..."
    key="xxxx"
    base_url = "http://localhost:3000"

    #connect to mongodb server and create database...
    client = MongoClient()
    db= client.scf_data

    print "........uploading cities"
    ## upload cities
    for city in db.cities.find({"id":city["id"]}):
        city_exists_url = base_url+"/api/city_exists/"
        city_exists_url += str(city["id"]) + "?key=" + key
        response = requests.get(city_exists_url)
        result = json.loads(response.text)
        if result["message"] == "false":
            city_upload_url = base_url+"/api/create_city"
            city_upload_url += "?key="+key
            city_upload_params = {"city":{
                "id_":city["id"],
                "name":city["name"],
                "lng":city["lng"],
                "lat":city["lat"]
            }}
            response=requests.post(
                city_upload_url,
                json=city_upload_params)
        else:
            continue

    print "........uploading streets"
    ## upload streets
    for street in db.streets.find({"city_id":city["id"]}):
        street_exists_url = base_url+"/api/street_exists/"
        street_exists_url += str(street["id"]) + "?key=" + key
        response = requests.get(street_exists_url)
        result = json.loads(response.text)
        if result["message"] == "false":
            street_upload_url = base_url+"/api/create_street"
            street_upload_url += "?key="+key
            street_upload_params = {"street":{
                "id_":street["id"],
                "name":street["name"],
                "city_id":street["city_id"],
                "length":street["length"]
            }}
            response=requests.post(street_upload_url,json=street_upload_params)
        else:
            continue

    print "........uploading request types"
    ## upload request types
    for request_type in db.request_types.find({"city_id":city["id"]}):
        request_type_exists_url = base_url+"/api/request_type_exists/"
        request_type_exists_url += str(request_type["id"]) + "?key=" + key
        response = requests.get(request_type_exists_url)
        result = json.loads(response.text)
        if result["message"] == "false":
            request_type_upload_url = base_url+"/api/create_request_type"
            request_type_upload_url += "?key="+key
            request_type_upload_params = {"request_type":{
                "id_":request_type["id"],
                "name":request_type["name"],
                "city_id":request_type["city_id"]
            }}
            response=requests.post(
                request_type_upload_url,
                json=request_type_upload_params)
        else:
            continue

    print "........uploading issues"
    ## upload issues
    issues_cursor = db.issues.find({"city_id":city["id"]},no_cursor_timeout=True)
    n_issues=issues_cursor.count()
    count=0
    for issue in issues_cursor:
        count+=1.
        print "...progress:",str(count/n_issues)+'\r',
        issue_exists_url = base_url+"/api/issue_exists/"
        issue_exists_url += str(issue["id"]) + "?key=" + key
        response = requests.get(issue_exists_url)
        result = json.loads(response.text)
        if result["message"] == "false":
            issue_upload_url = base_url+"/api/create_issue"
            issue_upload_url += "?key="+key
            issue_upload_params = {"issue":{
                "id_":issue["id"],
                "request_type_id":issue["request_type_id"],
                "city_id":issue["city_id"],
                "lng":issue["lng"],
                "lat":issue["lat"],
                "created_at":issue["created_at"],
                "status":issue["status"],
                "street_id":issue["street_id"]
            }}
            response=requests.post(
                issue_upload_url,
                json=issue_upload_params)
        elif result["message"] == "true":
            issue_update_url = base_url+"/api/update_issue"
            issue_update_url += "?key="+key
            issue_update_params = {"issue":{
                "id_":issue["id"],
                "status":issue["status"]
            }}
            response=requests.patch(
                issue_update_url,
                json=issue_update_params)
    issues_cursor.close()

    print "........uploading batches"
    ## upload latest batch
    for batch in db.batches.find({"city_id":city["id"]}):
        batch_exists_url = base_url+"/api/batch_exists/"
        batch_exists_url += str(batch["id"]) + "?key=" + key
        response = requests.get(batch_exists_url)
        result = json.loads(response.text)
        if result["message"] == "false":
            batch_upload_url = base_url+"/api/create_batch"
            batch_upload_url += "?key="+key
            batch_upload_params = {"batch":{
                "id_":batch["id"],
                "created_at":str(batch["created_at"]),
                "city_id":city["id"]
            }}
            response=requests.post(
                batch_upload_url,
                json=batch_upload_params)

        print "........uploading clusters"
        ## upload clusters and clusters issues with latest batch id
        for cluster in db.clusters.find({"batch_id":batch["id"]}):
            cluster_exists_url = base_url+"/api/cluster_exists/"
            cluster_exists_url += str(cluster["id"]) + "?key=" + key
            response = requests.get(cluster_exists_url)
            result = json.loads(response.text)
            if result["message"] == "false":
                cluster_upload_url = base_url+"/api/create_cluster"
                cluster_upload_url += "?key="+key
                cluster_upload_params = {"cluster":{
                    "id_":cluster["id"],
                    "batch_id":cluster["batch_id"],
                    "request_type_id":cluster["request_type_id"],
                    "city_id":cluster["city_id"],
                    "score":cluster["score"]
                }}
                response=requests.post(cluster_upload_url,json=cluster_upload_params)
            else:
                continue
            ## upload clusters_issues for each cluster
            for cluster_issue in db.clusters_issues.find(
                {"cluster_id":cluster["id"]}):

                cluster_issue_upload_url = base_url+"/api/create_cluster_issue"
                cluster_issue_upload_url += "?key="+key
                cluster_issue_upload_params = {"cluster_issue":{
                    "cluster_id":cluster_issue["cluster_id"],
                    "issue_id":cluster_issue["issue_id"]
                }}
                response=requests.post(
                    cluster_issue_upload_url,
                    json=cluster_issue_upload_params)
            else:
                continue

        else:
            continue
