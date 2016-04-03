import sys
sys.path.append("get_clusters")
from get_clusters import get_clusters
import json

with open('city_data/cities.json','r') as f:
    city=json.load(f)[0]

get_clusters(city)
