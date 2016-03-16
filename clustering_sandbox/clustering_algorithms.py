#!Clustering Algorithms

## Design clustering algorithms here that
## will be tested out on the data.

## Each clustering algorithm should be specified
## as a function that takes as arguments
## a list of longitude coordinates, a list of
## latitude coordinates and the city info, and returns
## a list of "clusters", each cluster being a list
## of indices of the coordinates involved...


## imports:
import math
from sklearn.cluster import KMeans
from sklearn.cluster import AgglomerativeClustering
from sklearn.cluster import AffinityPropagation
import numpy as np
import os

## used to find distances between points:
import geopy
from geopy import distance

import csv

import sys
sys.path.append("resources/mcl")
## function to translate between
## cluster labels and nested indices
def labels_to_index(cluster_labels):
    cluster_indices=[]
    for label in list(set(cluster_labels)):
        cluster_index = (cluster_labels==label).nonzero()[0].tolist()
        cluster_indices.append(cluster_index)
    return cluster_indices

## here's k-means clustering as an example of
## how to construct the clustering algorithm
def k_means_clustering(lngs,lats,city):
    city_lat=city["lat"];
    city_lng=city["lng"]
    ## scale the longitudinal axis to appriximate
    ## cartesian coordinates...
    lngs = np.array(lngs)*math.cos(city_lat)

    ## using n_issues/5 to determine k
    ## not the most objective method, but its a start...
    kmeans = KMeans(n_clusters = int(city_area/7290000.))
    kmeans.fit(np.array([lngs,lats]).transpose())

    cluster_labels = np.array(kmeans.labels_)
    ## use labels_to_index function to get
    ## output from cluster labels...
    return labels_to_index(cluster_labels)
	
	

def mcl(lngs,lats,city):
    city_lng=city["lng"]
    city_lat=city["lat"]
    ## generate graph
    graph=[]
    used_inds=[]
    for i in range(len(lngs)):
        for j in range(i+1,len(lngs)):
            distance=geopy.distance.vincenty(
					tuple([lngs[i],lats[i]]),
					tuple([lngs[j],lats[j]])).feet
            if distance<200:
                graph.append([i,j,distance])
    with open("mcl_data/mcl_input_data.tsv","w") as f:
		for row in graph:
			f.write(str(row[0])+"\t"+str(row[1])+"\t"+str(row[2])+"\n")
    os.system("mcl mcl_data/mcl_input_data.tsv --abc -o mcl_data/mcl_output_data.tsv")
    output_data=[]
    with open("mcl_data/mcl_output_data.tsv","r") as f:
        tsvin = csv.reader(f,delimiter='\t')
        for row in tsvin:
            int_row=[]
            for ind in row:
                int_row.append(int(ind))
            output_data.append(int_row)
    return output_data

	
def agglom(lngs, lats, city):
	city_area = city["area"]
	
	city_lng=city["lng"]
	city_lat=city["lat"]
	lngs = np.array(lngs)*math.cos(city_lat)
	
	agglomerative = AgglomerativeClustering(n_clusters = int(city_area/7290000.))
	agglomerative.fit(np.array([lngs, lats]).transpose())
	cluster_labels = np.array(agglomerative.labels_)
	
	return labels_to_index(cluster_labels)

def affinityprop(lngs, lats, city):
	city_area = city["area"]
	city_lng = city["lng"]
	city_lat = city["lat"]
	lngs = np.array(lngs)*math.cos(city_lat)
	
	affinity = AffinityPropagation(damping=0.5, max_iter=200, convergence_iter=15, copy=True, preference=None, affinity='euclidean', verbose=False) 
	affinity.fit(np.array([lngs, lats]).transpose())
	cluster_labels = np.array(affinity.labels_)
	
	return labels_to_index(cluster_labels)
	
	
	