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
# xs = ['0.5', '0.6', '0.7', '0.75', '0.8', '0.83', '0.86', '0.9', '0.93', '0.95']

# users = [34812, 35194, 49418, 49447, 78825, 78831, 87820]
# errors = [0.0451, 0.0438, 0.0238, 0.0229, 0.1955, 0.1954, 0.00006]
# thresholds = [0.8, 0.83, 0.85, 0.87, 0.9, 0.92, 0.95]

# error_cnt = [196488, 191954, 72201, 73558, 21437, 21432, 6]
# error_cnt = [ 193781, 189219, 70936, 72242, 70123,20867,20862, 6 ]
# tot_cnt = [4358134, 4383160, 3028218, 3209231, 3300335, 109637, 109682, 89219]
# errors = [float(error_cnt[i]) / tot_cnt[i] for i in xrange(len(error_cnt))]

# xs = [str(users[i])+'_'+ str(thresholds[i]) for i in xrange(len(users))]
total_device = 90443

# xs = [str(users[i] for i in xrange(len(users)))]
# print xs

# 90% certainty
# errors = [9076, 6378, 6353, 6119, 2296, 2293, 2290, 6]
# users = [35194, 49418, 49447, 50471, 78825, 78817, 78831, 87820]

# 100% certainty
thresholds = [0.8, 0.83, 0.85, 0.87, 0.88, 0.885, 0.89, 0.893, 0.895, 0.897, 0.9, 0.91, 0.92, 0.95]
errors = [9128, 9102, 6379, 6354, 6340, 6315, 6120, 3859, 3817, 3765, 2299, 2296, 2293, 6]
users = [34812, 35194, 49418, 49447, 49587, 49748, 50471, 56901, 57116, 57626, 78825, 78817, 78831, 87820 ]
ratios = [float(errors[i]) / users[i] for i in xrange(len(users))]


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
# ax.plot(xs, ratios, 'yo-', color='red')




plt.text (0.6,0.9, "total_device # = 90443",  transform = ax.transAxes, style='italic')
# plt.legend('total_device: 90443')
plt.savefig(os.path.join(path, 'majority_user_error_ratio.png'))
#  #[3991288, 3340251, 1688106, 903537, 282299, 126379, 84136, 24543, 23021, 0]

# plt.figure()
# # errors = [3991288, 3340251, 1688106, 903537, 282299, 126379, 84136, 24543, 23021, 0]
# # total_cnt = 4554137
# error_ratios = [float(error) / total_cnt for error in errors]
# plt.plot(xs, error_ratios, 'yo', color='red')
# plt.xlabel('threshold')
# plt.ylabel('error ratios')
# plt.title('error-ratio-vs-threshold')
# plt.savefig(os.path.join(path, 'error_ratio_threshold.png'))




'''
	Result:

    Threshold = 0.95
    ================
    bucket 1: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate 	average_#devices_per_user
	17         147588      70481       90443           0.000116        1.28

	bucket 2:
	error_cnt 	total_cnt 	user_cnt  	total_device	error_rate  average_#devices_per_user
     17          147600     70483       90443           0.000115        	1.28

	bucket 3:
	error_cnt  	total_cnt  	user_cnt  	total_device	error_rate  average_#devices_per_user
	6          89219       87820       90443           0.00006         1.03
     
    
    Threshold = 0.92
	================
	bucket 1: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate 	average_#devices_per_user
	667195		1139003		46166		90443			0.5858		1.9590

	bucket 2: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate 	average_#devices_per_user
	665043		1151113		44063		90443			0.5777		2.0526

	bucket 3: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate 	average_#devices_per_user
	21432		109682		78831		90443			0.1954		1.1473



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


	Threshold = 0.83
	=================
	bucket 1: 
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate  average_#devices_per_user
	3158165		85691910	7031		90443			0.0369		12.8635

	bucket 2:
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate  average_#devices_per_user
	3218804		90401001	6596		90443			0.0356		13.7118

	bucket 3:
	error_cnt  	total_cnt  	user_cnt  	total_device   	error_rate  average_#devices_per_user
	191954		4383160		35194		90443			0.0438		2.5698

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




'''
	With geolocation threshold = 0.025, bucket group 3

	Threshold = 0.95
	================
	6,89219,87820,90443

	Threshold = 0.92
	================
	20862,109682,78831,90443

	Threshold = 0.91
	================

	Threshold = 0.9
	===============
	20867,109637,78825,90443

	Threshold = 0.89
	================
	70123,3300335,50471,90443

	Threshold = 0.87
	===============
	72242,3209231,49447,90443

	Threshold = 0.85
	================
	70936,3028218,49418,90443

	Threshold = 0.83
	===============
	189219,4383160,35194,90443

	Threshold = 0.8
	===============
	193781,4358134,34812,90443


'''



'''
	Mimic President Election Method with 90% certainty


	Threshold = 0.95
	================
	6,87820,90443

	Threshold = 0.92
	================
	2290,78831,90443

	Threshold = 0.91
	==============
	2293,78817,90443

	Threshold = 0.9
	================
	2296,78825,90443

	Threshold = 0.89
	================
	6119,50471,90443

	Threshold = 0.87
	==============
	6353,49447,90443

	Threshold = 0.85
	================
	6378,49418,90443

	Threshold = 0.83
	================
	9076,35194,90443


'''


'''
	Mimic President Election Method with 100% certainty

	Threshold = 0.95
	================
	6,87820,90443

	Threshold = 0.92
	================
	2293,78831,90443

	Threshold = 0.91
	==============
	2296,78817,90443

	Threshold = 0.9
	================
	2299,78825,90443

	Threshold = 0.897
	=================
	3765,57626,90443

	Threshold = 0.895
	===============
	3817,57116,90443

	Threshold = 0.893
	================
	3859,56901,90443

	Threshold = 0.89
	================
	6120,50471,90443

	Threshold = 0.885
	=================
	6315,49748,90443

	Threshold = 0.88
	===============
	6340,49587,90443

	Threshold = 0.87
	==============
	6354,49447,90443

	Threshold = 0.85
	================
	6379,49418,90443

	Threshold = 0.83
	================
	9102,35194,90443

	Threshold = 0.8
	===============
	9128,34812,90443

'''
