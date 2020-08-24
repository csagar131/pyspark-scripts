from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MostPopularHero")
sc = SparkContext(conf = conf)


def parseLine(line):
	fields = line.split(' ')
	hero_id = int(fields[0])
	return (hero_id,len(fields)-1)


def parseLineMarvel(line):
	fields = line.split("\"")
	return (int(fields[0]),fields[1].encode('utf8'))


lines = sc.textFile("C:/pyspark-scripts/MarvelGraph.txt")

marvel_names = sc.textFile("C:/pyspark-scripts/MarvelNames.txt")
marvel_names_rdd = marvel_names.map(parseLineMarvel)

# (id,number)
# (345,10)
# (345,2)
hero_rdd = lines.map(parseLine)
total_count = hero_rdd.reduceByKey(lambda x,y : x+y)

# (12,345)
flipper = total_count.map(lambda x: (x[1],x[0]))
max_count_hero = flipper.max()


result = marvel_names_rdd.lookup(max_count_hero[1])[0]

print(result.decode(),max_count_hero[0])


