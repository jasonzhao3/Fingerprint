MapReduce Command Line:
=======================

00_state_filter:
...


01_profile_extraction:
=====================
# beacon
./elastic-mapreduce --create --stream --enable-debugging --num-instances 8 --master-instance-type m1.xlarge --slave-instance-type c1.xlarge --name "Build Beacon Profile" --mapper "s3n://cs341-yume-dp/CA_profile/mapper_beacon_profile_majority.py" --reducer "s3n://cs341-yume-dp/reducer_identical.py" --log-uri "s3n://cs341-yume-dp/CA_profile/logs/" --output "s3n://cs341-yume-dp/CA_profile/beacon_profile/" --input "s3n://cs341-yume-dp/CA_record/CA_beacon_record/" 

# request
./elastic-mapreduce --create --stream --enable-debugging --num-instances 10 --master-instance-type m1.xlarge --slave-instance-type c1.xlarge --name "Build Request Profile" --mapper "s3n://cs341-yume-dp/CA_profile/mapper_request_profile_majority.py" --reducer "s3n://cs341-yume-dp/reducer_identical.py" --log-uri "s3n://cs341-yume-dp/CA_profile/logs/" --output "s3n://cs341-yume-dp/CA_profile/request_profile/" --input "s3n://cs341-yume-dp/CA_record/CA_request/" 


# join
./elastic-mapreduce --create --stream --args -input,"s3n://cs341-yume-dp/CA_profile/beacon_profile/" --enable-debugging --num-instances 10 --master-instance-type m1.xlarge --slave-instance-type c1.xlarge --name "Join Beacon and Request Majority Profile" --mapper "s3n://cs341-yume-dp/mapper_identical.py" --reducer "s3n://cs341-yume-dp/CA_profile/reducer_join_profile_majority.py" --log-uri "s3n://cs341-yume-dp/CA_profile/logs/" --output "s3n://cs341-yume-dp/CA_profile/join_profile/" --input "s3n://cs341-yume-dp/CA_profile/request_profile/" 




02_lsh:
==========
./elastic-mapreduce --create --stream --enable-debugging --num-instances 10 --master-instance-type m1.xlarge --slave-instance-type c1.xlarge --name "LSH_v1.0" --mapper "s3n://cs341-yume-dp/CA_lsh/mapper_lsh_v1.0.py" --reducer "s3n://cs341-yume-dp/reducer_identical.py" --log-uri "s3n://cs341-yume-dp/CA_lsh/logs/" --output "s3n://cs341-yume-dp/CA_lsh/version1.0/" --input "s3n://cs341-yume-dp/CA_profile/join_profile/" 


03_evaluation  (distributed cache not working)
============
./elastic-mapreduce --create --stream --enable-debugging --num-instances 10 --master-instance-type m1.xlarge --slave-instance-type c1.xlarge --name "evaluation_v1.0" --mapper "s3n://cs341-yume-dp/CA_evaluation/mapper_eval_v1.0.py" --reducer "s3n://cs341-yume-dp/reducer_eval_v1.0.py" --log-uri "s3n://cs341-yume-dp/CA_evaluation/logs/" --output "s3n://cs341-yume-dp/CA_evaluation/version1.0/" --input "s3n://cs341-yume-dp/CA_lsh/version1.0/" --cache s3n://cs341-yume-dp/US-City-Location.csv#US-City-Location.csv --jobconf mapred.task.timeout=1000000



./elastic-mapreduce --create --stream --enable-debugging --num-instances 10 --master-instance-type m1.xlarge --slave-instance-type c1.xlarge --name "evaluation_v1.0" \
   --arg "-files" --arg "s3n://cs341-yume-dp/CA_evaluation/mapper_eval_v1.0.py,s3n://cs341-yume-dp/reducer_eval_v1.0.py" \
   --input s3n://cs341-yume-dp/CA_lsh/version1.0/ \
   --output s3n://cs341-yume-dp/CA_evaluation/version1.0/ \
   --mapper mapper_eval_v1.0.py \
   --reducer reducer_eval_v1.0.py \
   --cache s3n://cs341-yume-dp/US-City-Location.csv#US-City-Location.csv --jobconf mapred.task.timeout=1000000