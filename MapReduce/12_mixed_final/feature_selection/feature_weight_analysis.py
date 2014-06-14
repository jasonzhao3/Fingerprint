from __future__ import division
import os
from os import listdir
from os.path import isfile, join
from collections import Counter
import re
import snap
import hashlib

def combine_files(path):
	# if isfile(join(path,f)) 
	file_names = [ f for f in listdir(path) if isfile(join(path,f))]

	ls = []
	for file_name in file_names:
		file_name = join(path, file_name)
		# print file_name
		with open (file_name) as f:
			for line in f:
				line = line.strip()
				line = re.sub('[()]','', line)
				if (line):
					ls.append(line)
	return ls

def strip_to_get_nodes (str):
	nodes = line.split('\t')[0]
	nodes = nodes.strip('('')')
	edge = nodes.split(',')
	start = edge[0].strip().strip('\'')
	end = edge[1].strip().strip('\'')
	return start, end

def mergeCluster(start, end, flag):
    startInd = -1
    #print startInd
    if (start in indMap):
        startInd = indMap[start]
   
    finishInd = -1
    if (end in indMap):
        finishInd = indMap[end]
    
    if finishInd == startInd and startInd == -1:
        # no index found   
        newSet = set()
        newSet.add(start)
        newSet.add(end)
        clusters.append(newSet)
        clusters_valid.append(flag)
        indMap[start] = len(clusters) - 1
        indMap[end] = len(clusters) - 1
        # now start and end will be in len(clusters) - 1
        
    elif finishInd != startInd:
        if finishInd == -1:
            clusters[startInd].add(end)
            indMap[end] = startInd
            clusters_valid[startInd] = clusters_valid[startInd] and flag
        elif startInd == -1:
            clusters[finishInd].add(start)
            indMap[start] = finishInd
            clusters_valid[finishInd] = clusters_valid[finishInd] and flag
        else:
            # merging
            for elem in clusters[finishInd]:
                clusters[startInd].add(elem)
                indMap[elem] = startInd
                clusters_valid[startInd] = clusters_valid[startInd] and clusters_valid[finishInd]
            clusters[finishInd] = None
            clusters_valid[finishInd] = False # desserted


# '''
# 	Combine result
# '''
# path = '../../../../local_data/graph_data/edge/'




def build_user_map():
	user_map = dict()
	for file_name in file_names:
		file_name = join(path, file_name)
		with open (file_name) as f:
			for line in f:
				line = line.strip()
				key, val = line.split('\t')
				# only analyze request length here
				if (len(val.split(',')) == REQUEST_LENGTH):
					if key not in user_map:
						user_map[key] = [val]
					else:
						user_map[key].append(val)
	return user_map	



SIM_THRESHOLD = 0.7
REQUEST_LENGTH = 28


path = '../../../../local_data/final_mix/correct_user_v3.2/'
file_names = [ f for f in listdir(path) if isfile(join(path,f))]
print ("start process")

user_map = build_user_map()


for key in user_map.keys():
	print key, user_map[key]
	break





# with open('wcc', 'w') as f:
# 	for i in xrange(len(clusters)):
# 		if clusters[i]:
# 			temp_val = ','.join(clusters[i])
# 			value = temp_val + "|" + str(clusters_valid[i])
# 			# print ("%d%s%s" %(i, '\t', value))
# 			f.write(str(i) + '\t' + value)
# 			f.write('\n')



# # print G.GetNodes()
# Wccs = snap.TCnComV()
# snap.GetWccs(G, Wccs)

# cnt = 0
# with open('wcc', 'w') as f:
# 	for CnCom in Wccs:
# 	    node_list = [node_map[node] for node in CnCom]
# 	    f.write(str(cnt) + '\t')
# 	    f.write(','.join(node_list))
# 	    f.write('\n')
# 	    cnt += 1
# 	f.write(str(Wccs.Len()))


# cnt = 0
# CmtyV = snap.TCnComV()
# modularity = snap.CommunityCNM(G, CmtyV)
# with open('community', 'w') as f:
# 	f.write('Total number of nodes is ' + str(G.GetNodes()) + '\n')
# 	for Cmty in CmtyV:
# 	    print "Community: ", cnt
# 	    node_list = [node_map[node] for node in Cmty]
# 	    f.write(str(cnt) + '\t')
# 	    f.write(','.join(node_list))
# 	    f.write('\n')
# 	    cnt += 1
# 	f.write("total commmunity number is" + str(CmtyV.Len()))
# 	f.write("The modularity of the network is" + str(modularity))
# print CmtyV.Len()


# with open('distribution_v5.1.json', 'w') as f:
# 	json.dump(distribution_json, f)

# with open('similarity_v5.1.json', 'w') as f:
# 	json.dump(sim_json, f)
