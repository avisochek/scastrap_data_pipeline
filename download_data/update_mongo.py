import pymongo
from pymongo import MongoClient
from scf_data_download import download_issues,download_request_types
import os
import json

def get_city(city):
    #connect to mongodb server and create database...
    client = MongoClient()
    db= client.scf_data

    ## update city if necessary, either way,
    ## and return city bounds
    if not db.cities.find_one({"id":city["id"]}):
        db.cities.insert_one({
            "id":city["id"],
            "name":city["name"],
            "organizations":city["organizations"],
            "lat":city["lat"],
            "lng":city["lng"],
            "area":city["area"],
            "bounds":city["bounds"]})

def get_request_types(city):
    print "getting request types"
    #connect to mongodb server and create database...
    client = MongoClient()
    db= client.scf_data

    ## insert new request types into database
    ## at the same time, extract request_type_ids
    request_types = download_request_types(city)
    for request_type in request_types:
        if db.request_types.find({"id":request_type["id"]}).count()==0:
            db.request_types.insert_one(request_type)

def get_issues(city):
    #connect to mongodb server and create database...
    client = MongoClient()
    db= client.scf_data

    request_type_ids = [rt["id"] for rt in db.request_types.find()]
    ## 3. get issues
    print "getting issues..."
    issues = download_issues(request_type_ids,city)

    if not issues:
        return None

    ## 4. insert issues into database
    print "inserting issues into database..."
    for issue in issues:
        ## check for overlaps,
        ## and update existing issues
        ## at the same time...
        if not db.issues.find_one({"id":issue["id"]}):
            issue["street_id"]="A"
            db.issues.insert_one(issue)
        else:
            db.issues.update_one(
                {"id":issue["id"]},{"$set":
                {"status":issue["status"]}})
