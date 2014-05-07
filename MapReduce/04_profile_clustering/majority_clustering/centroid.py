import math
CITY_IND = 4

def cal_jaccard (record1, record2):
    num = 0
    denom = 0    
    for i in range(len(record1)):
        if (record1[i].lower() != "null" or record2[i].lower() != "null") and (record1[i].lower() != "n/a" or record2[i].lower() != "n/a") and (record1[i].lower() != "na" or record2[i].lower() != "na"): 
            denom = denom +1
            if record1[i] == record2[i]:
                num = num + 1   
    return num / denom
 
def cal_cosine(record1, record2):
    cross = 0.0
    norm1 = 0.0
    norm2 = 0.0
    for i in range(len(record1)):
        if (record1[i].lower() != "null" and record2[i].lower() != "null") and (record1[i].lower() != "n/a" and record2[i].lower() != "n/a") and (record1[i].lower() != "na" and record2[i].lower() != "na"):
            r1 = float(record1[i])
            r2 = float(record2[i])            
            cross += r1*r2
            norm1 += r1*r1
            norm2 += r2*r2
            
    denom = math.sqrt(norm1) * math.sqrt(norm2)
    if denom != 0:
        #print cross / denom
        return cross / denom
    else:
        return 0.0

def getSimilarity(profile1, profile2):
	x_list = profile1.split(',')
	y_list = profile2.split(',')
	request1 = [x_list[i] for i in range(15) if i != CITY_IND]
	request2 = [y_list[i] for i in range(15) if i != CITY_IND]
	beacon1 = x_list[15:-1]
	beacon2 = y_list[15:-1]
	score = 0.7 * cal_jaccard(request1, request2) + 0.3 * cal_cosine(beacon1, beacon2)
	return score

def getClustroid(cluster):
	maxSim = 0
	centroid = cluster[0]
	for i in range(0,len(cluster)):
		simTot = 0
		for j in range(0,len(cluster)):
			if j != i:
				simTot += getSimilarity(cluster[i],cluster[j])
		if simTot > maxSim:
			maxSim = simTot
			centroid = cluster[i]
	return centroid

if __name__ == "__main__":
	a = "297,70,4449,false,Riverside,4,Charter Communications,0_0,97.90.194.224,22,3,1,none,1633338080,1,0.19298245614,0.19298245614,0.19298245614,0.184210526316,0.184210526316,0.0526315789474,01pblTGrpVqNCR"
	b = "297,70,4405,false,Los Alamitos,4,Verizon FiOS,0_0,108.13.32.219,59,3,1,none,1812799707,1,0.216216216216,0.202702702703,0.202702702703,0.189189189189,0.189189189189,0.0,02SloaTEsUVbeY"
	c = "1267,227,7571,false,Long Beach,4,Charter Communications,84_0;83_0,75.128.34.121,22,0,0,N/A,1266688633,1,0.206106870229,0.198473282443,0.198473282443,0.198473282443,0.198473282443,0.0,04V4jIJGBS8ZeD"
	d = [a,b,c]
	print getClustroid(d)
