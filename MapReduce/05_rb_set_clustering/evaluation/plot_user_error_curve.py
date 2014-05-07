import os
import pylab as plt

path = '../../../../local_data/'


'''
 	cluster naming convention:
 	==========================
 	1. output_cluster_eval_0.8: original cluster with 3 bands
 	2. output_cluster_eval_0.8_3: Mimic President Election Method with 90% certainty
 	3. output_cluster_eval_0.9_4: Mimic President Election Method with 100% certainty
'''


'''
Data Preparation
'''
total_device = 90443
thresholds = [0.75, 0.77, 0.8, 0.83, 0.85, 0.87, 0.875, 0.88, 0.89, 0.9, 0.91]
errors = [9494, 9006, 7283, 5306, 3597, 2795, 2869, 2528, 1384, 561, 476]
users = [33368, 38398, 48980, 57505, 64434, 69010, 77214, 80117, 85341, 88153, 88434]
ratios = [float(errors[i]) / users[i] for i in xrange(len(users))]

# thresholds = [0.75, 0.77, 0.8, 0.83, 0.85, 0.87, 0.88]
# errors = [34359, 29123, 21974, 15194, 10940, 8530, 4466]
# devices = 90443
# ratios = [float(errors[i]) / devices for i in xrange(len(errors))]
# users = [33368, 38398, 48980, 57505, 64434, 69010, 80117]

'''
	Plot 
'''
f = plt.figure()
ax = f.add_subplot(111)

ax.tick_params(axis='both', labelsize=8)

xs = range(0, len(users))
plt.xticks(xs, users)
plt.xlabel('#user')
plt.ylabel('error user ratio')
plt.grid()
# plt.title('majority_user_error')
# ax2.xticks(xs, thresholds)
ax.plot(xs, ratios, 'yo-', color = 'red')

ax2 = ax.twiny()
plt.xticks(xs, thresholds)
plt.xlabel('#threshold')
plt.grid()
plt.text (0.6,0.9, "total_device # = 90443",  transform = ax.transAxes, style='italic')

plt.savefig(os.path.join(path, 'set_user_error_ratio.png'))
# plt.savefig(os.path.join(path, 'set_device_error_ratio.png'))



'''
	Error user evaluation with 100% certainty
	================
output_election_eval_0.75 	9494 	33368 90443
output_election_eval_0.77 	9006 	38398 90443
output_election_eval_0.8 	7283 	48980 90443
output_election_eval_0.83 	5306 	57505 90443
output_election_eval_0.85 	3597 	64434 90443
output_election_eval_0.87 	2795 	69010 90443
output_election_eval_0.875 	2869 	77214 90443
output_election_eval_0.88 	2528 	80117 90443
output_election_eval_0.89 	1384 	85341 90443
output_election_eval_0.9 	561 	88153 90443
output_election_eval_0.91 	476 	88434 90443

	Error device evaluation
	================
output_clustroid_eval_0.75 34359 90443 33368 90443
output_clustroid_eval_0.77 29123 90443 38398 90443
output_clustroid_eval_0.8 21974 90443 48980 90443
output_clustroid_eval_0.83 15194 90443 57505 90443
output_clustroid_eval_0.85 10940 90443 64434 90443
output_clustroid_eval_0.87 8530 90443 69010 90443
output_clustroid_eval_0.88 4466 90443 80117 90443

'''
