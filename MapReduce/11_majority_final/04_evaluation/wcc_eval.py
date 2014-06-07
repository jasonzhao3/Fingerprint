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


'''
	Combine result
'''
path = '../../../../local_data/graph_data/edge/'
# path = 's3n://cs341-yume-dp/CA_lsh_merged_band/lsh_band_step2_v3.3/'

file_names = [ f for f in listdir(path) if isfile(join(path,f))]

node_map = {}
id_map = {}
G = snap.TUNGraph.New()
node_idx = 0
for file_name in file_names:
	file_name = join(path, file_name)
	with open (file_name) as f:
		for line in f:
			line = line.strip()
			start, end = strip_to_get_nodes(line)
			if start not in id_map:
				id_map[start] = node_idx
				node_map[node_idx] = start
				G.AddNode(node_idx)
				node_idx += 1
			if end not in id_map:
				id_map[end] = node_idx
				node_map[node_idx] = end
				G.AddNode(node_idx)
				node_idx += 1

			G.AddEdge(id_map[start], id_map[end])
	break

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


cnt = 0
CmtyV = snap.TCnComV()
modularity = snap.CommunityCNM(G, CmtyV)
with open('community', 'w') as f:
	f.write('Total number of nodes is ' + str(G.GetNodes()) + '\n')
	for Cmty in CmtyV:
	    print "Community: ", cnt
	    node_list = [node_map[node] for node in Cmty]
	    f.write(str(cnt) + '\t')
	    f.write(','.join(node_list))
	    f.write('\n')
	    cnt += 1
	f.write("total commmunity number is" + str(CmtyV.Len()))
	f.write("The modularity of the network is" + str(modularity))
print CmtyV.Len()


# with open('distribution_v5.1.json', 'w') as f:
# 	json.dump(distribution_json, f)

# with open('similarity_v5.1.json', 'w') as f:
# 	json.dump(sim_json, f)
