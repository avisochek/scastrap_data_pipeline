#!update_local_db_and_upload.py
###first update local mongodb database,
###then get new street names and clusters,
###then upload to application database.
import json
import sys
sys.path.append("download_data/")
sys.path.append("get_clusters/")
sys.path.append("get_streets/")
sys.path.append("upload_data/")
from update_mongo import get_city,get_request_types,get_issues
from get_clusters import get_clusters, new_batch
from get_streets import get_streets
from bulk_upload import bulk_upload

print "updating local db..."
## 1. read in city names
with open("city_data/cities.json","r") as f:
    cities_list=json.load(f)


## 2. iterate through cities in list
## updating local db in the process...
for city in cities_list:
    #get_city(city)
    #get_request_types(city)
    #get_issues(city)
    #get_streets(city)
    get_clusters(city)
    
    ## 3. upload data to app database
    bulk_upload(city)
