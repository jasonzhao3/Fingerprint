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
 	   >> script: 01_profile_extraction/beacon
 	   >> size: ~15M
 	   >> time: ~5min

 	2) Request profile extraction:
 	   >> script: 01_profile_extraction/request/*
 	      majority-based and set-based
 	   >> size: ~250M
 	   >> time: ~25min

3. 





