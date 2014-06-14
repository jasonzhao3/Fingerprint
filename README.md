Documentation for CS341 Project
===============================
1. Git Repo Maintainance:
   - push local branch to remote: git push (remote) (branch)
   - track remote branch: git checkout -b (branch) (remote/branch)
   - merge branches: git checkout (mergeto branch); git merge (mergefrom branch)
   - delete branch: git branch -d (branch)
   - gitignore file: first "git rm -r --cached .", then "git add .", then "git commit -am ...."

2. Project Management:
   - We usually collaraboratively work on the develop branch. You are encouraged to create your own local branch to play around.
   - Everytime before you resume coding, please pull the repo first, in case other temmember has modifed the code.




Understanding Large graphs: similarity & summarization

Danai Koutra (CMU)

1. graph similarity: two graphs with correspond nodes, but different edges 
   1) simple features: 
      - edge overlap (EO)
   	  - vertex/edge overlap
   	  - vertex ranking (pagerank)
   	  - maximum common subgraph

   2) Complex features:
   	  - signature similarity (fingerprint -- by Garcia-Molina '10'
   	    (what's the intuition?)
   	  - Axioms





