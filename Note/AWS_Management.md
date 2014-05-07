AWS S3 and EMR Management
=========================
1. Prefiltering:  
  
  Get all California Sessions (Records): both request and beacon with selected features
  >> script: state_filter/*
  >> input: 
  			s3://cs341-06-data/yume/request/2014/04/merge06-13/     (~30G)
  			s3n://cs341-06-data/yume/beacon/2014/04/merge06-13/     (~3T)
  >> output: CA_request_record (old: output-final);  CA_beacon_record (filter-beacon-output-2)
  >> size: request: ~50G; beacon: ~500M
  >> time: request: ~2h; beacon: ~20 min
  >> old cluster name: fingerprint_filter  &  Beacon Filter 

 
 2. Profile Extraction:
 

