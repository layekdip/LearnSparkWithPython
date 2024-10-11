from pyspark import SparkConf, SparkContext
import collections

run_config = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=run_config)

lines = sc.textFile("../data_sources/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
