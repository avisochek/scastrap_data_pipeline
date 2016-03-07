import pymongo
from pymongo import MongoClient
from get_street_lengths import get_street_lengths
from get_street_name import get_street_name

def get_streets(city):
    print "getting streets..."
    ## connect to mongodb server and create database...
    client = MongoClient()
    db= client.scf_data

    ## iterate over new streets,
    ## either searching the data
    ## for new streets or assigning
    ## issue to existing street id
    new_street_names=[]
    new_street_ids=[]
    new_street_copies=[]
    new_street_copies_ids=[]
    print ".......getting street names..."
    for issue in db.issues.find({"city_id":city["id"]}):
        if issue["street_id"]==-1:
            street_name=get_street_name(issue["address"])
            street=db.streets.find_one({
                "city_id":city["id"],
                "name":street_name})
            if street:
                db.issues.update_one(
                    {"id":issue["id"]},{"$set":
                    {"street_id":street["id"]}})
            elif street_name not in new_street_names:
                ## store new street names in a list to process all at once
                ## to save time (parsing the xml file takes forever...)
                new_street_names.append(street_name)
                new_street_ids.append(issue["id"])
            else:
                ## store copies of new streets in a separate list
                new_street_copies.append(street_name)
                new_street_copies_ids.append(issue["id"])

    ## street_lengths is a dictionary, with keys as the name of
    ## each street that was found, and values representing the
    ## length of each street
    street_lengths = get_street_lengths(new_street_names,city)
    for ind in range(len(new_street_names)):
        if new_street_names[ind] in street_lengths.keys():
            if db.streets.find().count()>0:
                last_street_id=db.streets.find().sort("id",pymongo.DESCENDING)[0]["id"]
            else:
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

    ## update copies of new street names separately...
    for ind in range(len(new_street_copies)):
        if new_street_copies[ind] in street_lengths.keys():
            street_id=db.streets.find_one({
                "city_id":city["id"],
                "name":new_street_copies[ind]})["id"]
            db.issues.update_one(
                {"id":new_street_copies_ids[ind]},
                {"$set":{"street_id":street_id}})
        else:
            db.issues.update_one(
                {"id":new_street_copies_ids[ind]},
                {"$set":{"street_id":0}})
