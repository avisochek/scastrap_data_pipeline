import pymongo
from pymongo import MongoClient
client=MongoClient()
db = client.scf_data
from get_street_info import get_street_name,get_street_length
from get_street_name_old import get_street_name as get_street_name_old
from get_street_lengths_old import get_street_lengths
# total=0
# match=0
# length=db.issues.find({"status":{"$in":["Open","Acknowledged"]}}).count()
# for issue in db.issues.find({"status":{"$in":["Open","Acknowledged"]}}):
#     total+=1
#     if get_street_name(issue)==get_street_name_old(issue["address"]):
#         match+=1
#     print total/float(length)
# print float(match)/total
city = db.cities.find_one()
city["streets_file"]="new_haven.osm"
street_name="Whitney Avenue"
print get_street_length(street_name,city)
print get_street_lengths([street_name],city)
