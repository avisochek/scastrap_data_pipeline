import sys, os
import csv

f = open(sys.argv[1], 'r')
g = open(sys.argv[2], 'w')
h = open(sys.argv[3], 'w')

data = []

#stores each cluster as a list within the larger data list (list of lists)
for line in f:
	data.append(line.strip().split('\t'))

#for each cluster list within the larger data list...
for n in range(0, len(data)):
	#...writes the cluster number to file...
	clusterid = "cluster"+str(n+1)+"\n"
	g.write(clusterid)
	#...each individual term within a cluster list is written on a new line
	for y in range(0, len(data[n])):
		incident = data[n][y]+'\n'
		g.write(incident)
	#the clusters are separated by a block of white space	
	g.write('\n'+'\n'+'\n'+'\n'+'\n'+'\n'+'\n'+'\n'+'\n'+'\n')
	
g.close()

"""(1) The code above this point essentially transposes each horizontal cluster vertically,
labels each cluster, separates each cluster by white space, and saves it all to a file."""