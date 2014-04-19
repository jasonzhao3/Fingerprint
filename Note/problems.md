MapReduce Problem Collection:
=============================
1.  Error: # of failed Map Tasks exceeded allowed limit.
   => Cause: data format non-uniform, some lines may miss something?
             Not configuration related.

2.  Error: # of failed Reduce Tasks exceeded allowed limit.
   => Cause1: Missing a top line starts with #, indicating to Hadoop it's a python file.
   => Cause2: Uncalled functions, avoid them. (possible reason)
 
