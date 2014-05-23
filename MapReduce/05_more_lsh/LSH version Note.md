LSH version Note:

1. version1.0: numBuckets = 100,000

2. version2.0: numBuckets = 1,000,000

3. version3.0: numBuckets = 1,500,000

4. version4.0: numBuckets = 8,000,000



LSH iteration version Note:
1. version1.0: numBucket =  ['2m', '4m', '6m', '8m', '10m', '12m']
			   HASH_STRING_CONST_MOD = 1000000
			   geo_threshold = 0.02

2. version2.0: numBucket =  ['2m', '4m', '6m', '8m', '10m', '12m']
			   HASH_STRING_CONST_MOD = 12000000
			   geo_threshold = 0.02

3. version3.0: numBucket =  ['5m', '6m', '7m', '8m', '9m', '10m', '11m', '12m', '13m', 								'14m', '15m', '16m', '17m', '18m', '19m', '20m']
			   HASH_STRING_CONST_MOD = 20000000
			   geo_threshold = 0.03
			   evaluation: if there is one within 25 miles, then success



LSH Evaluation Test: 
====================
1. version 1.0: 
	- input: majority_version_3.0
	- Parameter: 
	   - geo: 0.03  as long as one out of this range, wrong
	   - time: as long as one timestamp duplicated, wrong
	   - hid: if no any intersection, wrong

2. version 2.0:
	- input: majority_version_3.0
	- Parameter: 
	   - geo: 0.02  as long as one out of this range, wrong
	   - time: as long as one timestamp duplicated, wrong
	   - hid: if no any intersection, wrong

2. version 3.0:
	- input: majority_version_3.0
	- Parameter: 
	   - geo: 0.04  as long as one out of this range, wrong
	   - time: as long as one timestamp duplicated, wrong
	   - hid: if no any intersection, wrong

4. version 4.0:
	- input: majority_version_3.0
	- Parameter: 
	   - geo: 0.02  if all pairs out of this range, wrong
	   - time: as long as one timestamp duplicated, wrong
	   - hid: if no any intersection, wrong