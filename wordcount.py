from pyspark import SparkConf, SparkContext
import re
from collections import OrderedDict

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def parseLine(line):
	return re.compile(r'\W+',re.UNICODE).split(line.lower()) # a regex split function


lines = sc.textFile("C:/SparkCourse/Book.txt")
words = lines.flatMap(parseLine) #transformation

map_words = words.map(lambda x:(x,1))
count_words = map_words.reduceByKey(lambda x,y:x+y)


revesed_rdd = count_words.map(lambda x: (x[1],x[0]))


results = revesed_rdd.sortByKey().collect()



for  result in results:
	print(result[1],str(result[0]))
		

