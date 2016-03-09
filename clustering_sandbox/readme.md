#Clustering Sandbox
###This folder offers us a place to experiment with different clustering methods to see what works best. It includes a script for generating and storing clusters (get_test_clusters.py), a file to code and designate different clustering methods (clustering algorithms.py) and an html file for viewing the results.

### In order to use this module:
#####(make sure you have a good internet connection)

### 1. Download SeeClickFix data to your local database by running update_local_db.py. You may need to view the main readme to find out how to do this.

### 2. Write a new clustering algorithm function in clustering_algorithms.py using existing examples as a template.

### 3. Configure get_test_clusters.py by adding the id of your desired city (find this in city_data/cities.json), specifying the desired base name for the output files, and importing your desired clustering algorithm.

### 4. Run get_test_clusters.py. The output JSON files will be located in the cluster_data folder.

### 5. Open up cluster_viewer.html in a web browser, select a file to observe and browse through the designated clusters.
