#######
#######
#######
#congfig... set parameters below

city_id = 3039 # New Haven, CT is the default
base_filename = "mcl7"
## import the clustering algorithm that we want to use...
from clustering_algorithms import mcl as cluster_issues## kmeans is the default
## set minimum number of issues for each set of clusters
min_issues=1
##set the cluster diameter in feet
cluster_diameter=2000
#######
#######
#######

import json ## use this for file io

## setup pymongo to interact with database
## where data is stored
import pymongo
from pymongo import MongoClient
client = MongoClient()
db = client.scf_data

## pull city data from the database..
city = {
"name":"Detroit, MI",
"organizations":["Detroit Water & Sewerage Dept.","City of Detroit"],
"id":73249,
"lng":-83.0442361839428,
"lat":42.3578559608962,
"osm":"detroit.osm",
"area":109088179203,
"bounds":[[[-83.146824,42.319537],[-83.153524,42.328037],[-83.156524,42.327137],[-83.156625,42.327538],[-83.156724,42.328737],[-83.156824,42.329837],[-83.157025,42.336937],[-83.153124,42.337637],[-83.151024,42.338937],[-83.148824,42.344737],[-83.147624,42.352037],[-83.135624,42.352137],[-83.138124,42.355737],[-83.138224,42.359337],[-83.138724,42.364137],[-83.139724,42.364037],[-83.140024,42.367337],[-83.141124,42.367737],[-83.140924,42.366637],[-83.145324,42.366637],[-83.145824,42.371437],[-83.145139,42.371112],[-83.138924,42.368737],[-83.139224,42.378837],[-83.135024,42.380537],[-83.126824,42.383737],[-83.125324,42.381337],[-83.119224,42.383537],[-83.122424,42.388237],[-83.119724,42.389437],[-83.110823,42.392737],[-83.107323,42.387937],[-83.103923,42.383037],[-83.099323,42.384637],[-83.102146,42.387947],[-83.097623,42.389837],[-83.088623,42.393137],[-83.079722,42.396537],[-83.076123,42.397638],[-83.074236,42.398243],[-83.07348,42.398486],[-83.071122,42.395737],[-83.068922,42.393137],[-83.066122,42.389937],[-83.061822,42.390437],[-83.055866,42.381668],[-83.055322,42.380938],[-83.055222,42.380738],[-83.054522,42.379538],[-83.04051,42.384585],[-83.043027,42.388271],[-83.043722,42.389538],[-83.044321,42.390337],[-83.042121,42.390237],[-83.042221,42.393137],[-83.042421,42.396737],[-83.042421,42.397237],[-83.042621,42.404737],[-83.054021,42.404637],[-83.057122,42.409037],[-83.058022,42.408737],[-83.059422,42.410937],[-83.058622,42.411337],[-83.058822,42.411737],[-83.061422,42.415536],[-83.063323,42.418637],[-83.067022,42.423936],[-83.068522,42.426036],[-83.073323,42.433036],[-83.078623,42.440236],[-83.083323,42.446736],[-83.083324,42.447237],[-83.063723,42.447337],[-83.053523,42.447637],[-83.044223,42.447837],[-83.039123,42.447937],[-83.034421,42.448136],[-83.024522,42.448337],[-83.006821,42.448836],[-83.00472,42.449136],[-83.00252,42.449036],[-82.985521,42.449137],[-82.970919,42.449836],[-82.96772,42.449737],[-82.956318,42.450136],[-82.943117,42.450436],[-82.941617,42.450436],[-82.941217,42.450436],[-82.940617,42.450436],[-82.928675,42.450444],[-82.917616,42.450636],[-82.914716,42.450736],[-82.911048,42.450736],[-82.904916,42.450736],[-82.892015,42.450936],[-82.888415,42.451036],[-82.887815,42.451036],[-82.884014,42.451036],[-82.880814,42.451036],[-82.87936,42.451119],[-82.877314,42.451236],[-82.8705723992744,42.451236],[-82.870347,42.450888],[-82.8833811085873,42.4155447819221],[-82.886113,42.408137],[-82.8863699462228,42.4070310140845],[-82.888413,42.398237],[-82.894013,42.389437],[-82.898413,42.385437],[-82.9002834549594,42.3846194249324],[-82.9062678373413,42.3820036539374],[-82.9123392471649,42.3793498432846],[-82.915114,42.378137],[-82.919114,42.374437],[-82.928815,42.359437],[-82.928615,42.3591328101135],[-82.928615,42.358237],[-82.931115,42.357437],[-82.937915,42.356537],[-82.938515,42.357137],[-82.942215,42.356537],[-82.943115,42.356337],[-82.943015,42.355837],[-82.952816,42.355037],[-82.956016,42.354837],[-82.958316,42.358037],[-82.959316,42.358237],[-82.957116,42.354837],[-82.960316,42.354437],[-82.969017,42.355637],[-82.971117,42.356437],[-82.973328,42.355509],[-82.979717,42.353537],[-82.989818,42.351237],[-82.996118,42.348438],[-82.996049,42.347304],[-83.000118,42.345038],[-83.011319,42.337838],[-83.025719,42.331838],[-83.042821,42.327039],[-83.065382,42.320123],[-83.075521,42.314438],[-83.078521,42.312938],[-83.081621,42.310438],[-83.093,42.298921],[-83.099222,42.292538],[-83.099722,42.292538],[-83.102922,42.288738],[-83.103074,42.288056],[-83.103122,42.287838],[-83.110022,42.276438],[-83.109922,42.274038],[-83.112322,42.269638],[-83.113322,42.266138],[-83.114622,42.263938],[-83.115422,42.264138],[-83.119422,42.261638],[-83.117022,42.260638],[-83.117822,42.259138],[-83.118822,42.257838],[-83.125622,42.260938],[-83.119022,42.256938],[-83.120222,42.255838],[-83.125122,42.258238],[-83.125522,42.257838],[-83.120922,42.255338],[-83.127122,42.248638],[-83.135623,42.244138],[-83.140823,42.242438],[-83.143723,42.240939],[-83.147423,42.234939],[-83.147523,42.229639],[-83.148123,42.229539],[-83.148323,42.228239],[-83.143723,42.222639],[-83.141023,42.218139],[-83.141523,42.214339],[-83.144023,42.208939],[-83.147423,42.202739],[-83.148623,42.19954],[-83.150531,42.192989],[-83.154224,42.18524],[-83.160225,42.185141],[-83.167724,42.18504],[-83.179625,42.18494],[-83.180125,42.19154],[-83.180325,42.199439],[-83.180925,42.213839],[-83.181225,42.221139],[-83.194125,42.221039],[-83.200108,42.220811],[-83.200283,42.223254],[-83.200364,42.224669],[-83.200527,42.22814],[-83.200686,42.230494],[-83.200816,42.232426],[-83.201727,42.23244],[-83.201927,42.236939],[-83.202126,42.245938],[-83.202726,42.245638],[-83.196525,42.256138],[-83.184586,42.270537],[-83.183925,42.271538],[-83.179825,42.269338],[-83.176026,42.266439],[-83.171324,42.264938],[-83.170068,42.264707],[-83.167924,42.266538],[-83.166624,42.268338],[-83.164924,42.270038],[-83.157725,42.278639],[-83.161024,42.281438],[-83.163324,42.285238],[-83.163724,42.286238],[-83.167124,42.289838],[-83.164091,42.290756],[-83.158519,42.292085],[-83.156092,42.29544],[-83.155946,42.295954],[-83.150787,42.296455],[-83.149464,42.295147],[-83.147024,42.292938],[-83.145524,42.293238],[-83.144724,42.293538],[-83.144223,42.293838],[-83.142523,42.294438],[-83.141023,42.296238],[-83.140323,42.299738],[-83.142224,42.305438],[-83.140824,42.306838],[-83.140023,42.308438],[-83.140824,42.310938],[-83.146824,42.319537]]]
}

## iterative over request types, getting
## issue coordinates for all open issues in each
for request_type in db.request_types.find({"city_id":city_id}):
    request_type_id = request_type["id"]
    lngs=[]
    lats=[]
    issue_ids =[]
    for issue in db.issues.find(
        {"request_type_id":request_type_id,
        "status":{"$in":["Open","Acknowledged"]}}):

        lngs.append(issue["lng"])
        lats.append(issue["lat"])
        issue_ids.append(issue["id"])
    ## check that there are a reasonable number of
    ## issues in the collection, say 50
    if len(lngs)>min_issues:
        ## get clusters
        clusters_ind = cluster_issues(lngs,lats,city,cluster_diameter)
        ## the last cluster should represent
        ## all of the issues that aren't used
        used_issues_ind=[]
        for cluster_ind in clusters_ind:
            used_issues_ind+=cluster_ind
        unused_issues_ind=[ind for ind in range(len(issue_ids)) if not ind in used_issues_ind]
        print request_type_id,len(used_issues_ind+unused_issues_ind)
        clusters_ind.append(unused_issues_ind)

        ## translate array of indices into array of issue ids
        clusters = []
        for cluster_ind in clusters_ind:
            cluster=[]
            for ind in cluster_ind:
                cluster.append({
                    "issue_id":issue_ids[ind],
                    "lat":lats[ind],
                    "lng":lngs[ind]})
            clusters.append(cluster)

        ## write cluster data to a json file
        output_filepath = "cluster_data/"+base_filename
        output_filepath += "_rtid-"+str(request_type_id)
        output_filepath += "_cityid-"+str(city["id"])
        with open(output_filepath,"w") as f:
            json.dump(clusters,f)
    else:
        print "request type #",request_type_id,"had less than 50 issues"
