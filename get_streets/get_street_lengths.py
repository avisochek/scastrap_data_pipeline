#!get_street_lengths
###determines the length of each street
###using osm data.

import pickle
import numpy as np
from lxml import etree

## imports for finding distance between nodes
import geopy
from geopy import distance

## imports for checking weather node is within city bounds
from shapely.geometry import Polygon as pg
from shapely.geometry import Point

def check_bounds(lngs,lats,bounds_polygon):
    nodes_within_bounds = []
    for ind in range(len(lngs)):
        if bounds_polygon.contains(Point(lngs[ind],lats[ind])):
            nodes_within_bounds.append(True)
        else:
            nodes_within_bounds.append(False)
    return nodes_within_bounds

def get_street_lengths(street_names,city):
    street_names=street_names
    data = etree.parse(open("city_data/osm/"+city["streets_file"]))

    ## first, go through the data for each way,
    ## and extract the ids for each node involved.
    print "     getting street nodes..."
    street_nodes={}
    for element in data.iter("way"):
        names = element.findall("./tag[@k='name'].")
        if len(names)>0:
            name=names[0].attrib["v"]
            if name in street_names:
                if not name in street_nodes.keys():
                    street_nodes[name]=[]
                refs=[]
                for node in element.iter('nd'):
                    refs.append(node.attrib['ref'])
                street_nodes[name].append(refs)

    print "     getting street coords..."
    street_coords={}
    ## get street coords...
    ## Search the data for each node to get its coordinates
    for name in street_nodes.keys():
        street_coords[name]=[]
        for i in range(len(street_nodes[name])):
            street_coords[name].append([])
            for j in range(len(street_nodes[name][i])):
                street_coords[name][i].append([])
                node = data.find("./node[@id='"+street_nodes[name][i][j]+"']")
                street_coords[name][i][j]=[node.attrib["lon"],node.attrib["lat"]]


    ## create a polygon object to check if a node is within city bounds
    bounds_polygon = pg(*[city["bounds"]])


    print "     getting street bounds..."
    ### determine weather or not a node is within the provided city boundary.
    street_coords_within_bounds={}
    for name in street_coords.keys():
        street_coords_within_bounds[name]=[]
        for i in range(len(street_coords[name])):
            lngs=[float(j[0]) for j in street_coords[name][i]]
            lats=[float(j[1]) for j in street_coords[name][i]]
            nodes_within_bounds=check_bounds(lngs,lats,bounds_polygon)
            street_coords_within_bounds[name].append(nodes_within_bounds)

    print "     getting street lengths..."
    ## finally, we calculate the street length by adding
    ## the length between each pair of consecutive nodes
    ## in each segment, and adding the length of each segment,
    ## only considering those nodes within new haven.
    street_lengths={}
    if len(street_coords)>0:
        for name,segments in street_coords.items():
            # todo: possible conditional here to eliminate street lengths of 0
            street_lengths[name]=0
            for i in range(len(segments)):
                if len(segments[i])>1:
                    for j in range(1,len(segments[i])):
                        distance_between_nodes=geopy.distance.vincenty(tuple(street_coords[name][i][j]), tuple(street_coords[name][i][j-1])).miles
                        ### find the length of the street strictly within new haven...
                        current_node_within_bounds=street_coords_within_bounds[name][i][j]
                        last_node_within_bounds=street_coords_within_bounds[name][i][j-1]
                        if current_node_within_bounds or last_node_within_bounds:
                            if current_node_within_bounds and last_node_within_bounds:
                                street_lengths[name]+=distance_between_nodes
                            else:
                                street_lengths[name]+=distance_between_nodes/2.

    return street_lengths
