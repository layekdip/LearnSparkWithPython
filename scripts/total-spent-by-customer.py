from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return int(fields[0]), float(fields[2])

input_data = sc.textFile("../data_sources/customer-orders.csv")
mappedInput = input_data.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y).sortByKey()

results = totalByCustomer.collect()

for result in results:
    print(result)
