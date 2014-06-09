from random import randint
import random
import json

'''
  build map
'''
def build_value_map(data_file):
  # build possible values map
  value_map = {}
  with open(data_file, "r") as f:
    for line in f:
        line = line.rstrip()
        key, value = line.split('\t')
        value_list = value.split(',')
        value_map[int(key)] = value_list
  return value_map

def build_perm_map(data_file):
  perm_map = {}
  with open(data_file, "r") as f:
    for line in f:
        line = line.rstrip()
        key, value = line.split('\t')
        str_list = value.split(',')
        perm_list = [int(str_list[i]) for i in range(len(str_list))]
        perm_map[int(key)] = perm_list
  return perm_map
    
# value_map: key:attribute_idx,  val: list of possible attributes
# perm_map: key:attribute_idx, val: index permutation list
# search_map: nested dictionary
# - outside layer: key: attribute_idx   val: search_dictionary
# - inside(search_dictionary)  key: attribute_alue   val: permutation_idx
def build_search_map(value_map, perm_map):
  search_map = dict()
  for key in value_map.keys():
    val_list = value_map[key]
    perm_idx_list = perm_map[key]
    comb_list = zip(perm_idx_list, val_list)
    comb_list.sort(key=lambda t:t[0])
    inner_dict = dict()
    for item in comb_list:
      inner_dict[item[1]] = item[0]
    search_map[key] = inner_dict
  return search_map


search_map_list = []
value_map = build_value_map ('./local_data/permutation/attr_count_output')
print "finish building value_map"

perm_map = build_perm_map('./local_data/permutation/permutation_output')
search_map_list.append(build_search_map(value_map, perm_map))
print "finish building first perm map"

perm_map = build_perm_map('./local_data/permutation/permutation_output_2')
search_map_list.append(build_search_map(value_map, perm_map))
print "finish building second perm map"

perm_map = build_perm_map('./local_data/permutation/permutation_output_3')
search_map_list.append(build_search_map(value_map, perm_map))

# print search_map_list

with open('perm_json.json', 'w') as f:
  json.dump(search_map_list, f, indent=4, separators=(',', ': '))

