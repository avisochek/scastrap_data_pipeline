import requests
import json
import math
import numpy as np

## imports for checking weather
## street node is within city bounds
from shapely.geometry import Polygon as pg
from shapely.geometry import Point

def get_street_name(issue):
    base_url="http://localhost:3535/nominatim/reverse"
    params={
        "format":"json",
        "lat":issue["lat"],
        "lon":issue["lng"],
        "limit":1}
    r = requests.get(base_url,params=params);
    result = r.json()
    if "address" in result.keys():
        if "road" in result["address"].keys():
            return result["address"]["road"]
    else:
        return False

def distance_between_nodes(node1,node2):
    R=3961.
    dlon = (math.pi/180.)*(node1[0]-node2[0])
    dlat = (math.pi/180.)*(node1[1]-node2[1])
    a=(math.sin(dlat/2.))**2.+np.abs(math.cos(node1[1])*math.cos(node2[1])*((math.sin(dlon/2.))**2.))
    c=2.*math.atan2(math.sqrt(a),math.sqrt(1.-a))
    d=R*c
    return d

def get_segment_length(geojson,city):
    bounds_polygons = [pg(*[bounds]) for bounds in city["bounds"]]
    data = geojson["coordinates"]
    length=0
    while len(data)>1 and isinstance(data[0],list):
        last_node = data.pop(0)
        current_node = data[0]
        ## find how many of the two nodes are in the city
        n_in_bounds=False
        n_in_bounds+=False
        for bounds_polygon in bounds_polygons:
            if bounds_polygon.contains(Point(last_node[0],last_node[1])):
                n_in_bounds+=1
                break
        for bounds_polygon in bounds_polygons:
            if bounds_polygon.contains(Point(current_node[0],current_node[1])):
                n_in_bounds+=1
                break

        if n_in_bounds > 0:
            distance = distance_between_nodes(last_node,current_node)
            if n_in_bounds ==1:
                length+=distance/2
            else:
                length+=distance
    return length

def get_street_length(street_name,city):
    base_url="http://localhost:3535/nominatim/search"
    params={
        "format":"json",
        "street":street_name,
        "polygon_geojson":1,
        "dedupe":0,
        "limit":100}
    ## retrieve street coordinate data from database
    r = requests.get(base_url,params=params);
    result = r.json()
    street_length=0
    for element in result:
        if "class" in element.keys():
            if element["class"]=="highway":
                if "geojson" in element.keys():
                    segment_length=get_segment_length(element["geojson"],city)
                    street_length+=segment_length

    if street_length>0:
        return street_length
    else:
        return False
