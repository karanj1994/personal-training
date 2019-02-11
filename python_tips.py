import os
import json
import argparse

my_file = os.popen("maprcli table region list -path /datalake/other/polarisprovider/polarisdatamovement/blue/perf/data/hcp_region -json")
with my_file as json_data:
    d = json.load(my_file)

# test = []
# for i in d["data"]:
#    test.append(i["logicalsize"])

# list comprehension
# logical_size = [i["logicalsize"] for i in d["data"]]
# number_of_rows = [i["numberofrows"] for i in d["data"]]
# fid = [i["fid"] for i in d["fid"]]

# dictionary definition (dictionary comprehension)
hbase_regions = {i["fid"]:(i["logicalsize"],i["numberofrows"]) for i in d["data"]}


def avg(d, key):
    # d.itervalues creates an interator on your dictionaries values (d.iterkeys would create an list of keys)
    to_compute = [rec[key] for rec in d.itervalues()]
    mean = sum(to_compute)/float(len(to_compute))
    return mean

avg_logical_size, avg_num_rows = avg(hbase_regions, 0), avg(hbase_regions, 1)

for key,val in hbase_regions.iteritems():
    print val

offending_ips = []
for k,v in hbase_regions.iteritems():
    if ((v[0] > 1000000000 and v[0] > avg_logical_size) or (v[1] > 100000 and v[1] > avg_num_rows)):
        print k, " will be split"
        os.system("maprcli table region split -path /datalake/other/polarisprovider/polarisdatamovement/blue/perf/data/hcp_region -fid " + k)

        # print stored input of variable in a string
        # print("hello, I am " + var_name)
        # print("hello, I am %s" % var_name)


#for key,val in hbase_regions.iteritems():
#    print val
