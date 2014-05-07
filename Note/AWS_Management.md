AWS S3 and EMR Management
=========================
1. Prefiltering:  
  
  Get all California Sessions (Records): both request and beacon with selected features
  >> script: 
  			     00_state_filter/*
  >> input: 
  			     s3://cs341-06-data/yume/request/2014/04/merge06-13/     (~30G)
  			     s3n://cs341-06-data/yume/beacon/2014/04/merge06-13/     (~3T)
  >> output: 
  			     CA_request_record (old: output-final);  
  			     CA_beacon_record (old: filter-beacon-output-2)
  >> output size: 
  			     request: ~50G; 
  			     beacon: ~500M
  >> time: 
  			     request: ~2h; 
  			     beacon: ~20 min
  >> old cluster name: 
            fingerprint_filter  
  			     Beacon Filter 

 
2. Profile Extraction: only extract device with more than 30 records

 	1) Beacon profile extraction:
 	   >> script: 
 	   		       	01_profile_extraction/beacon
 	   >> input:
 	   		       	CA_beacon_record (old: filter-beacon-output-2)
 	   >> size:   ~15M
 	   >> time:   ~5min

 	2) Request profile extraction:
 	   >> script: 
     			      01_profile_extraction/request/*
       		      majority-based and set-based
 	   >> input:
    		        CA_request_record (old: output-file)
 	   >> size:   ~250M
 	   >> time:   ~25min


3. Beacon-Request Profile Join:  either majority-based or set-based
	
  >> script:
			       02_profile_join
	>> input: 
		        >>> majority-based profile: 
		        >>> set-based profile: beacon_request_set_final/output_full/ 
  >> output:
             s3://cs341-yang/fingerprint/beacon_request_set_final/output_set_final


4. Profile Pair-pair comparison:
   >> LSH:
          input: 
          output: 
   >> evaluation: 
          input: s3://cs341-yang/fingerprint/beacon_request_lsh/output_1/
          output: s3://cs341-yang/fingerprint/beacon_request_lsh/lsh_eval_week


5. Profile Clustering:
   >> script: 04_profile_clustering/*




