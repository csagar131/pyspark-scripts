from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MostPopularMovie")
sc = SparkContext(conf = conf)


def send_to_brodcast():
	movie_dict = dict()
	with open("C:/pyspark-scripts/ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movie_dict[int(fields[0])] = fields[1]
		return movie_dict


movie_dict = sc.broadcast(send_to_brodcast())

lines = sc.textFile("C:/pyspark-scripts/ml-100k/u.data")
rdd = lines.map(lambda x:(int(x.split()[1])))

mapped_rdd = rdd.map(lambda x:(x,1))

reduced_rdd = mapped_rdd.reduceByKey(lambda x,y: x+y)

flipped = reduced_rdd.map(lambda x: (x[1],x[0]))

#(353,50)
results = flipped.sortByKey().map(lambda x: (movie_dict.value[x[1]],x[0])).collect()

for result in results:
	print(result)



# from pyspark import SparkConf, SparkContext

# def loadMovieNames():
#     movieNames = {}
#     with open("C:/pyspark-scripts/ml-100k/u.item") as f:
#         for line in f:
#             fields = line.split('|')
#             movieNames[int(fields[0])] = fields[1]
#     return movieNames

# conf = SparkConf().setMaster("local").setAppName("PopularMovies")
# sc = SparkContext(conf = conf)

# nameDict = sc.broadcast(loadMovieNames())

# lines = sc.textFile("C:/pyspark-scripts/ml-100k/u.data")
# movies = lines.map(lambda x: (int(x.split()[1]), 1))
# movieCounts = movies.reduceByKey(lambda x, y: x + y)

# flipped = movieCounts.map( lambda x : (x[1], x[0]))
# sortedMovies = flipped.sortByKey()

# sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

# results = sortedMoviesWithNames.collect()

# for result in results:
#     print (result)
