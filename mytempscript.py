from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)


def parseLine(line):
	fields = line.split(',')
	station_id = fields[0]
	t_field = fields[2]
	temp = (float(fields[3])* 0.1 * 9/5) + 32.0
	return (station_id,t_field,temp)


lines = sc.textFile("C:/SparkCourse/1800.csv")
rdd = lines.map(parseLine)
#(staion_id,t_field,temp)

tmin_data = rdd.filter(lambda x:'TMAX' in x[1])

kv_rdd = tmin_data.map(lambda x:(x[0],x[2]))
#(station_id,(t_field,temp))
# ('ITE00100554',-234))
# ('EZE00100082',-211))
# ('ITE00100554',-193))
# ('EZE00100082',-202))
# ('ITE00100554',-50))



min_temp = kv_rdd.reduceByKey(lambda x,y:max(x,y))
result = min_temp.collect()

for value in result:
 	print(value[0],round(value[1],2))



