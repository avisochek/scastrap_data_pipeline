#!update_local_db.py
###update local mongodb database,
###run this file to just to get data
###locally i.e. for experimentation
import json
import sys
sys.path.append("download_data/")
from update_mongo import get_city,get_request_types,get_issues

print "updating local db..."
## 1. read in city names
with open("city_data/cities.json","r") as f:
    cities_list=json.load(f)

## create a new batch
new_batch()

## 2. iterate through cities in list
## updating local db in the process...
for city in cities_list:
    city = get_city(city)
    get_request_types(city)
    get_issues(city)
