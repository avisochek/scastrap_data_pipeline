import pymongo
from pymongo import MongoClient
import requests
import json
#
def bulk_upload(city):
    print "uploading data..."
    key="xxxx"
    base_url = "http://localhost:3000"

    #connect to mongodb server and create database...
    client = MongoClient()
    db= client.scf_data

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

    print "........uploading issues"
    ## upload issues, first get all issues into array
    issues_cursor = db.issues.find({"city_id":city["id"]},no_cursor_timeout=True)
    n_issues=issues_cursor.count()
    count=0.
    issues=[]
    for issue in issues_cursor:
        issue_to_append={
            "id_":issue["id"],
            "city_id":city["id"],
            "request_type_id":issue["request_type_id"],
            "created_at":issue["created_at"],
            "status":issue["status"],
            "address":issue["address"],
            "street_id":0,
            "lng":issue["lng"],
            "lat":issue["lat"],
            "summary":issue["summary"],
            "description":issue["description"]}
        if "street_id" in issue.keys():
            issue_to_append["street_id"]=issue["street_id"]
        issues.append(issue_to_append)
    issues_cursor.close()
    ## upload in batches of 1000
    for ind in range((len(issues)/1000)+1):
        issues_to_upload=issues[(1000*ind):min((1000*(ind+1)),len(issues))]
        #print issues_to_upload[len(issues_to_upload)-1]
        count+=1000
        #print "asdf"
        print "...progress:",str(count/n_issues)+'\r',

        issue_upload_url = base_url+"/api/bulk_upsert_issue"
        issue_upload_url += "?key="+key
        issue_upload_params = {"issues":issues_to_upload}
        response=requests.post(
            issue_upload_url,
            json=issue_upload_params)

    print "........uploading batches"
    ## upload latest batch
    for batch in db.batches.find({"city_id":city["id"]}):
        batch_exists_url = base_url+"/api/batch_exists/"
        batch_exists_url += str(batch["id"]) + "?key=" + key
        response = requests.get(batch_exists_url)
        result = json.loads(response.text)
        if result["message"] == "false":

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
                clusters_issues = []
                for cluster_issue in db.clusters_issues.find(
                    {"cluster_id":cluster["id"]}):
                    clusters_issues.append({
                        "cluster_id":cluster["id"],
                        "issue_id":cluster_issue["issue_id"]})
                for ind in range((len(clusters_issues)/1000)+1):
                    clusters_issues_to_upload = clusters_issues[(1000*ind):min(1000*(ind+1),len(clusters_issues))]
                    clusters_issues_upload_url = base_url+"/api/bulk_upsert_cluster_issue"
                    clusters_issues_upload_url += "?key="+key
                    clusters_issues_upload_params = {"clusters_issues":clusters_issues_to_upload}
                    response=requests.post(
                        clusters_issues_upload_url,
                        json=clusters_issues_upload_params)

            ## upload batch once clusters are loaded
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

    print "........uploading city"
    ## upload cities
    city = db.cities.find_one({"id":city["id"]})
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
