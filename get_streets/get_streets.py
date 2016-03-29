import pymongo
from pymongo import MongoClient
from get_street_lengths import get_street_lengths
from get_street_name import get_street_name

def get_streets(city):
    print "getting streets..."
    ## connect to mongodb server and create database...
    client = MongoClient()
    db= client.scf_data

    ## Iterate over new issues.
    ## If street name is found, add to the list,
    ## otherwise assign a street_id of 0 to the issue.
    street_names=[]
    issue_ids=[]
    print ".......getting street names..."
    for issue in db.issues.find(
        {"city_id":city["id"],
        "street_id":-1}):

        street_name=get_street_name(issue)
        if street_name:
            street_names.append(street_name)
            issue_ids.append(issue["id"])
        else:
            db.issues.update_one(
                {"id":issue_ids[i]},{"$set":
                {"street_id":street["id"]}}))

    ## Iterate over street names of new issues.
    ## If street name with the current city is
    ## in the database, assign current issue the
    ## id of identified street, otherwise, find the
    ## new street and add it to the database...
    for ind in range(len(street_names)):
        street=db.streets.find_one({
            "city_id":city["id"],
            "name":street_name})

        if street:
            db.issues.update_one(
                {"id":issue_ids[i]},{"$set":
                {"street_id":street["id"]}})

        else:
            street_length = get_street_length(street_names[i],city)

            if street_length:
                if db.streets.find().count()>0:
                    last_street_id=db.streets.find_one(sort="id",-1)["id"]
                else:
                    ## Initialize streets db with
                    ## street 0
                    db.streets.insert_one({
                        "id":0,
                        "name":"none",
                        "city_id":"none",
                        "length":1
                    })
                    last_street_id=0

                db.streets.insert_one({
                    "id":last_street_id+1,
                    "name":new_street_names[ind],
                    "city_id":city["id"],
                    "length":street_lengths[new_street_names[ind]]})
                db.issues.update_one(
                    {"id":new_street_ids[ind]},
                    {"$set":{"street_id":last_street_id+1}})
            else:
                db.issues.update_one(
                    {"id":new_street_ids[ind]},
                    {"$set":{"street_id":0}})
