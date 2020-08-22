from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CSV2Parquet")
sc = SparkContext(conf = conf)

rdd = sc.textFile("C:/2015-summary.csv")
df = rdd.map(lambda line: line.split(","))
df.write.parquet('C:/2015-summary.parquet')