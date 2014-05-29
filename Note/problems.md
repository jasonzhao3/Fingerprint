MapReduce Problem Collection:
=============================
1.  Error: # of failed Map Tasks exceeded allowed limit.
   => Cause: data format non-uniform, some lines may miss something?
             Not configuration related.

2.  Error: # of failed Reduce Tasks exceeded allowed limit.
   => Cause1: Missing a top line starts with #, indicating to Hadoop it's a python file.
   => Cause2: Uncalled functions, avoid them. (possible reason)
 
3. stderr: 
java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 143
  at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:372)
  at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:586)
  at org.apache.hadoop.streaming.PipeReducer.reduce(PipeReducer.java:130)
  at org.apache.hadoop.mapred.ReduceTask.runOldReducer(ReduceTask.java:528)
  at org.apache.hadoop.mapred.ReduceTask.run(ReduceTask.java:429)
  at org.apache.hadoop.mapred.Child$4.run(Child.java:255)
  at java.security.AccessController.doPrivileged(Native Method)
  at javax.security.auth.Subject.doAs(Subject.java:415)
  at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1132)
  at org.apache.hadoop.mapred.Child.main(Child.java:249)

  syslog:
  2014-05-05 05:15:15,481 WARN org.apache.hadoop.streaming.PipeMapRed (main): java.io.IOException: Broken pipe
  at java.io.FileOutputStream.writeBytes(Native Method)
  at java.io.FileOutputStream.write(FileOutputStream.java:345)
  at java.io.BufferedOutputStream.write(BufferedOutputStream.java:122)
  at java.io.BufferedOutputStream.flushBuffer(BufferedOutputStream.java:82)
  at java.io.BufferedOutputStream.flush(BufferedOutputStream.java:140)
  at java.io.DataOutputStream.flush(DataOutputStream.java:123)
  at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:579)
  at org.apache.hadoop.streaming.PipeReducer.reduce(PipeReducer.java:130)
  at org.apache.hadoop.mapred.ReduceTask.runOldReducer(ReduceTask.java:528)
  at org.apache.hadoop.mapred.ReduceTask.run(ReduceTask.java:429)
  at org.apache.hadoop.mapred.Child$4.run(Child.java:255)
  at java.security.AccessController.doPrivileged(Native Method)
  at javax.security.auth.Subject.doAs(Subject.java:415)
  at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1132)
  at org.apache.hadoop.mapred.Child.main(Child.java:249)
  
  >> possible timeout / out of memory issue


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

2. AWS multiple input path
./elastic-mapreduce --create --stream --args -input,"s3n://cs341-yume-dp/input/input2/" --enable-debugging --num-instances 2 --master-instance-type m1.xlarge --slave-instance-type m1.xlarge --name "Test command line" --mapper "s3n://cs341-yume-dp/mapper_identical.py" --reducer "s3n://cs341-yume-dp/reducer_identical.py" --log-uri "s3n://cs341-yume-dp/logs/" --output "s3n://cs341-yume-dp/output" --input "s3n://cs341-yume-dp/input/input1/" 

Reference: 
http://www.henrycipolla.com/blog/2011/09/how-to-create-an-emr-job-with-multiple-inputs-using-the-ruby-client/


  Some waste of time:
  ===============
  1. for a single device, multiple city spread out over different places
  2. print numDevice as a key => will be bueried in the ocean of keys 
  3. print similarity distribution of correct and wrong => should separate buckets, otherwise 1 consumes too much!!


  Meet-up Questions:
  ===================
  1. Evaluation: our errror rate is the correct pair # / all pairs within a cluster -- is it a reasonable choice?
  2. Maybe loose our geolocation constraint??  -- we have tested it, no big difference
  3. content-based clustering?  => advertisement id 
  4. Clustering users in different bands? How to merge bands? 
  5. how to deal with online fingerprint??
