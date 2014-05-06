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
	Threshold = 0.92
	================
	

	Threshold = 0.9
	================
	bucket 1: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate 	average_#devices_per_user
	700995     	1185455    	45896		90443 	 	   	0.5914		1.9701

	bucket 2:
	error_cnt 	total_cnt 	user_cnt  	total_device	error_rate  average_#devices_per_user
	696147	  	1198165    	43691		90443			0.5810		2.0701

	bucket 3:
	error_cnt  	total_cnt  	user_cnt  	total_device	error_rate  average_#devices_per_user
	21437		109637	  	78825		90443			0.1955		1.1474

	Threshold = 0.87
	================
	bucket 1: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate 	average_#devices_per_user
	1604892		27355475	14564		90443			0.0587		6.2100

	bucket 2: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate 	average_#devices_per_user
	1669469		20884372	13736		90443			0.0799		6.5844

	bucket 3: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate 	average_#devices_per_user
	73558		3209231		49447		90443			0.0229		1.8210


	Threshold = 0.85
	=================
	bucket 1: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate  average_#devices_per_user
	1734903		28596002  	13972		90443	 	   	0.0607		6.4731

	bucket 2:
	error_cnt 	total_cnt	user_cnt  	total_device	error_rate  average_#devices_per_user
	1724806		17675893	13176		90443			0.0976		6.8642

	bucket 3:
	error_cnt  	total_cnt  	user_cnt  	total_device	error_rate  average_#devices_per_user
	72201		3028218		49418		90443			0.0238		1.8302


	Threshold = 0.8
	==================
	bucket 1: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate  average_#devices_per_user
	3712060		97051696	6135		90443			0.0382		14.7421

	bucket 2: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate  average_#devices_per_user
	3676928		98470302	5739		90443			0.0373		15.7594

	bucket 3: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate  average_#devices_per_user
	196488		4358134		34812		90443			0.0451		2.5980

'''
