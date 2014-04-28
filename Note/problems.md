MapReduce Problem Collection:
=============================
1.  Error: # of failed Map Tasks exceeded allowed limit.
   => Cause: data format non-uniform, some lines may miss something?
             Not configuration related.

2.  Error: # of failed Reduce Tasks exceeded allowed limit.
   => Cause1: Missing a top line starts with #, indicating to Hadoop it's a python file.
   => Cause2: Uncalled functions, avoid them. (possible reason)
 

 AWS User Document:
 ===================
1. Documents:
  1) Move Data with Amazon EMR
  2) Amazon EMR storage is different from S3, but EMR can directly access S3 just like a HDFS (i.e. by reference)
  3) Parallel Cluster: multiple clusters can access same S3 data simutaneously
  4) Spot Instances -- save money by bid
  5) CloudWatch can be used to monitor the running efficiency of the cluster 
  6) S3 has a limit of 200 transactions per second, so be careful about using it as a intermediate result
  7) Steps: only after previous step finishes, will the current step start. Chain steps rather than mapreduce tasks have better performance, as the intermediate data is store in the HDFS rather than S3.
  8) Amazon EMR preserves metadata information about cluster, and clean up it after two months.
  9) CLI default region is us-east-1
  10) 