TODO list:
============
1. Jaccard set/mixture based profile extraction 
   -- similar to majority based method, we should extract different metric to different features (i.e. jaccard set, ratios)
   -- Getting this will help cross-method evaluation

2. Implement multiple similarity metrics
   -- naive average method
   -- more complexed method?
   -- do cross evaluation of the two

3. Attributes in next step:
   - timestamp
   



 5. We focus on many-many problem, but it can be easily extended to many-one problem







 Output Files:
 1. fingerprint/output_final: all CA request record data with wanted attributes (~50G)
 2. fingerprint/filter-beacon-output2: all CA beacon record with wanted attributes (~500M)
 
 3. fingerprint/beacon_build_profile/ouput: with_1: request profile; without_1: beacon profile
 4. fingerprint/join_request_beacon/output2: joined profiles
 
 5. fingerprint/beacon_request_lsh/lsh_eval_week: for each threshold, error number based on final result (overall evaluation)




 Xinyuan Zhao (Anand): twitter data
 1. definition
 2. model: 
 	- mathematical formula
    - complexity of algorithm
 3. evaluation:
 	- compare two methods


 Michael Fang (Anand): fingerprint
 1.  CA: 6 million phones, 600 thousand tablets, 4.3 million ip address, 139 million sessions
 2. Algorithm: 
 	- ROCK algorithm (Guha, Rastagi & Shim 19999) -- content based approaches  (Behavior contents)
 	- identify households: => why???  trend and basis
 	- identify users: => how???
 	- Content-based
 3. graph should be clear, especially about the legend


 Time Series Anmaly Detection:
 =========
 1. average window - good for spike like
 2. Clustering
 3. hidden markov good for step-like pattern


Hierarchical structure
=======
A classical community detection algorithms
Graph construction 


Parkinson Severity classfication
==========
openSMILE  -- voice open source libarary


Predict breakout apps
==========


already scaled
>30 filter