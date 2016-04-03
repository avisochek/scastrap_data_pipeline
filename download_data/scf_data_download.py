#!scf_data_download
### Download data from the SeeClickFix
### api and return as a group of python
### lists representing the data

import urllib2
import json

## imports for checking weather issue is within city bounds
from shapely.geometry import Polygon as pg
from shapely.geometry import Point

# def download_city_bounds(city):
#     request_url="https://seeclickfix.com/api/v2/places/"+str(city["id"])
#     response=urllib2.urlopen(request_url)
#     return json.loads(response.read())["poly"]["coordinates"][0][0]


def download_request_types(city):
    ## get request types and ids from api
    request_types=[]
    request_url="https://seeclickfix.com/api/v2/issues/new?address="
    request_url+= "+".join(city["name"].split())
    response=urllib2.urlopen(request_url)
    request_types_dict=json.loads(response.read())
    for request_type in request_types_dict["request_types"]:
        if request_type["organization"] in city["organizations"]:
            request_type_id = request_type["url"].split("/request_types/")[1]
            request_type_name=request_type["title"]
            request_types.append({
                "id":request_type_id,
                "name":request_type_name,
                "city_id":city["id"]})
    return request_types


def download_issues(request_type_ids,city):

    ## generate the base url
    base_url_1="https://seeclickfix.com/api/v2/issues?"
    base_url_1+="status=open,acknowledged,closed,archived"
    base_url_1+="&per_page=100&request_types="

    ## initiate issues array
    issues=[]

    ## create polygon object in order to
    ## check to see that issues are within city
    ## boundaries before adding them.
    bounds_polygons=[pg(*[bounds]) for bounds in city['bounds']]

    ## iteratively get the data by request type
    ## and then by page number
    for request_type_id in request_type_ids:
        print ".......getting request type "+request_type_id
        base_url_2=base_url_1+str(request_type_id)+"&page="
        page=1
        data=[0]
        while len(data)>0:
            url=base_url_2+str(page)
            response=urllib2.urlopen(url)
            data = json.loads(response.read())["issues"]
            for document in data:
                ## here we check if issue coords are within city bounds
                for bounds_polygon in bounds_polygons:
                    if bounds_polygon.contains(Point(document["lng"],document["lat"])):
                        issues.append({
                            "id":document["id"],
                            "city_id":city["id"],
            		        "request_type_id":request_type_id,
                            "street_id":-1,
                            "created_at":document["created_at"],
                            "status":document["status"],
                            "address":document["address"],
                            "lng":document["lng"],
                            "lat":document["lat"]})
                        break
            page+=1

    return issues
