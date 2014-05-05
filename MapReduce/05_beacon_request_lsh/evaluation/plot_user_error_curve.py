import os
import pylab as plt

path = '../../../local_data/'

'''
Data Preparation
'''
xs = ['0.5', '0.6', '0.7', '0.75', '0.8', '0.83', '0.86', '0.9', '0.93', '0.95']
users = []
thresholds = []

xs = [users[i]+'_'+thresholds[i] for i in xrange(len(users))]
errors = []
total_cnt = 0


'''
	Plot 
'''
plt.figure()
plt.plot(xs, errors, 'yo', color='red')
plt.xlabel('#user_threshold')
plt.ylabel('number of error records')
plt.title('error-vs-threshold')
plt.savefig(os.path.join(path, 'error_threshold.png'))
#  #[3991288, 3340251, 1688106, 903537, 282299, 126379, 84136, 24543, 23021, 0]

plt.figure()
# errors = [3991288, 3340251, 1688106, 903537, 282299, 126379, 84136, 24543, 23021, 0]
# total_cnt = 4554137
error_ratios = [float(error) / total_cnt for error in errors]
plt.plot(xs, error_ratios, 'yo', color='red')
plt.xlabel('threshold')
plt.ylabel('error ratios')
plt.title('error-ratio-vs-threshold')
plt.savefig(os.path.join(path, 'error_ratio_threshold.png'))




'''
	Result:
	Threshold = 0.9
	================
	bucket 1: 
	error_cnt  total_cnt  user_cnt   error_rate  total_device
	700995     1185455    45896		 0.5914

	bucket 2:
	error_cnt  total_cnt  user_cnt   error_rate  total_device
	696147	   1198165    43691		 0.5810

	bucket 3:
	error_cnt  total_cnt  user_cnt   error_rate  total_device
	21437		109637	  78825		 0.1955
'''
