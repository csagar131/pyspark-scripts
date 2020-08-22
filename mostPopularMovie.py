from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MostPopularMovie")
sc = SparkContext(conf = conf)


lines = sc.textFile("C:/SparkCourse/ml-100k/u.data")
rdd = lines.map(lambda x:(int(x.split()[1])))

mapped_rdd = rdd.map(lambda x:(x,1))

reduced_rdd = mapped_rdd.reduceByKey(lambda x,y: x+y)

flipped = reduced_rdd.map(lambda x: (x[1],x[0]))

results = flipped.sortByKey().collect()

for result in results:
	print(result)










