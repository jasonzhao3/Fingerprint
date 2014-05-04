TODO list:
============
1. Jaccard set/mixture based profile extraction 
   -- similar to majority based method, we should extract different metric to different features (i.e. jaccard set, ratios)
   -- Getting this will help cross-method evaluation

2. Implement multiple similarity metrics
   -- naive average method
   -- more complexed method?
   -- do cross evaluation of the two

3. Grouping Devices into each user:
   -- after we get pair-pair
   -- mapper: 
      emit (key: pair[0], val: pair[1])  where sim(pair) > 0.8
      emit (key: pair[1], val: pair[0])
   -- reducer:
      calculate pair-pair similarity within value list,
      and emit (key: triple, val: min(similarity))
   -- similarly, we can do it for at most four devices



4. Evaluation curve
   -- mimic MMDS


 5. We focus on many-many problem, but it can be easily extended to many-one problem







 Output Files:
 1. fingerprint/output_final: all CA request record data with wanted attributes (~50G)
 2. fingerprint/filter-beacon-output2: all CA beacon record with wanted attributes (~500M)