import math
import numpy as np
import os
import csv

def mcl(lngs,lats,city, cluster_diameter):
    lat_to_feet_multiplier=288200.
    lng_multiplier=math.cos(city["lat"])
    city_lng=city["lng"]
    city_lat=city["lat"]
    ## generate graph
    graph=[]
    used_inds=[]
    for i in range(len(lngs)):
        for j in range(i+1,len(lngs)):
            distance_y=np.abs(lats[i]-lats[j])*lat_to_feet_multiplier
            distance_x=np.abs(lngs[i]-lngs[j])*lat_to_feet_multiplier*lng_multiplier
            distance=math.sqrt(distance_x**2+distance_y**2)
            # distance=geopy.distance.vincenty(
			# 	tuple([lngs[i],lats[i]]),
			# 	tuple([lngs[j],lats[j]])).feet
            if distance<cluster_diameter:
                graph.append([i,j,1-distance/(1.5*(cluster_diameter))])
    ## write graph to mcl input file
    with open("mcl_data/mcl_input_data.tsv","w") as f:
		for row in graph:
			f.write(str(row[0])+"\t"+str(row[1])+"\t"+str(row[2])+"\n")
    ## run mcl using command line
    os.system("mcl mcl_data/mcl_input_data.tsv -I 3 --abc -o mcl_data/mcl_output_data.tsv")
    output_data=[]
    ## read in output file from previous step
    with open("mcl_data/mcl_output_data.tsv","r") as f:
        tsvin = csv.reader(f,delimiter='\t')
        for row in tsvin:
            int_row=[]
            for ind in row:
                int_row.append(int(ind))
            output_data.append(int_row)
    return output_data
