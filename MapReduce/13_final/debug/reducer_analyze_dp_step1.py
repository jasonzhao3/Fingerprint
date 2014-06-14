#!/usr/bin/env python
from itertools import groupby
from operator import itemgetter
import sys
 
'''
   This map-reduce job is used to analyze delivery point correlated attributes

Step1:
------
   Mapper output format:
        attribute_value \t delivery_point

   Reducer output format:
        attribute  \t value_true/false

Step2:
------
    Mapper output format:
        attribute \t  supportNum_totNum

    Reducer: identical

'''

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

attr_list = [
       'domain_id',
       'placement_id',    
       'advertisement_id',      
       'census_DMA',  
       'publisher_id',
       'content_video_id',
       'service_provider_id',
       'key_value',
       'player_location_id', 
       'player_size_id',
       'page_fold_id',
       'ad_visibility',
       'ovp_version',
       'ovp_type',
       'hid',
       'is_on_premise',
       'audience_segment',
       'referrer_site',
       'network_id',
       'slot_type_id',
       'ad_request_id',
       'is_not_yume_white_list',
       #########################################
       'publisher_channel_id',       
       'content_profile_id',
       'is_pre_fetch_request',
       'service_provider_name',
       'slate_id',
       'zero_tracker',
       'twentry_five',
       'fifty',
       'seventry_five',
       'one_hundred',
       'volume percent',
       'time_location'

]
 
def is_full_support(dp_set):
    if (len(dp_set) == 5):
        return True
    elif (len(dp_set) == 4 and '0' not in dp_set):
        return True
    else:
        return False


def is_two_support(dp_set):
    if (len(dp_set) == 3):
      return True
    elif (len(dp_set) == 2 and '0' not in dp_set):
      return True
    else:
      return False

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)

    for current_attribute, group in groupby(data, itemgetter(0)):
        try:
            attr_idx, val = current_attribute.split(',')
            unique_dp_set = set()
            for key, curr_dp in group:
                unique_dp_set.add(curr_dp)
                if (is_two_support(unique_dp_set)):
                    break

            if (is_two_support(unique_dp_set)):
                print '%s%s%s' % (attr_list[int(attr_idx)], '\t', val + ',' + 'true')
            else:
                print '%s%s%s' % (attr_list[int(attr_idx)], '\t', val + ',' + 'false')

        except ValueError:
            # count was not a number, so silently discard this item
            pass
 
if __name__ == "__main__":
    main()

