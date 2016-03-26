import json
from matplotlib import pyplot as plt
import numpy as np

filepath="cluster_data/mcl5_rtid-373_cityid-3039"
with open(filepath,"r") as f:
    data=json.load(f)
plt.figure()
for cluster in data:
    color=np.random.rand(3,1)
    for issue in cluster:
        plt.plot(issue["lng"],issue["lat"],"o",c=color,alpha=1)
plt.show()
