beacon_profile = [   
    0, # domain id      
    1, # placement_id
    2, # advertisement_id
    3, # census_dma_id
    4, # publisher_channel_id
    5, # content_video_id
    6, # service provider id
    7, # key values 
    8, # player location 
    9, # player size
    10, # page fold
    11, # ad visibility
    12, # ovp version
    13, # ovp type
    14, # hid
    15, # is on-premise
    16, # audience segments
    ############################
    17, # slate id
    ############################     
    18, # zero_tracker
    19, # twentry_five
    20, # fifty
    21, # seventry_five
    22, # one_hundred
    23, # volume percent
    24, # time_location tuple
]


request_profile = [
               0, # domain_id - majority
               1, # placement_id
               2, # advertisement_id
               3, # census_DMA - majority
               4,  # publisher_id - set
               5, # content_video_id (skip 0)
               6, # service_provider_id - jaccard set
               7, # key_value - jaccard set
               8, # player_location_id 
               9, # player_size_id - jaccard set
               10, # page_fold_id - majority
               11, # ad_visibility
               12, # ovp_version  
               13, # ovp_type
               14, # hid
               15, # is_on_premise
               16, # audience_segment (skip NULL)
                ############
               17, # referrer_site (skip NULL)
               18, # network_id - set
               19, # slot_type_id - majority (low weight because too many 1)
               20, # ad_request_id
               21, # is_not_yume_white_list  - ratio of true
               22, # publisher_channel_id - (skip 0)
               23, # content_profile_id (skip null)
               24, # is_pre_fetch_request
               25, # service_provider_name  - majority         
               26, # time_location tuple
             ];


full_profile = [
               0, # domain_id - majority
               1, # placement_id
               2, # advertisement_id
               3, # census_DMA - majority
               4,  # publisher_id - set
               5, # content_video_id (skip 0)
               6, # service_provider_id - jaccard set
               7, # key_value - jaccard set
               8, # player_location_id 
               9, # player_size_id - jaccard set
               10, # page_fold_id - majority
               11, # ad_visibility
               12, # ovp_version  
               13, # ovp_type
               14, # hid
               15, # is_on_premise
               16, # audience_segment (skip NULL)
                ############
               17, # referrer_site (skip NULL)
               18, # network_id - set
               19, # slot_type_id - majority (low weight because too many 1)
               20, # ad_request_id
               21, # is_not_yume_white_list  - ratio of true
               22, # publisher_channel_id - (skip 0)
               23, # content_profile_id (skip null)
               24, # is_pre_fetch_request
               25, # service_provider_name  - majority         
             

            26, # slate id
            ############################     
            27, # zero_tracker
            28, # twentry_five
            29, # fifty
            30, # seventry_five
            31, # one_hundred
            32, # volume percent
            33, # time_location tuple
]