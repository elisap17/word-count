import sys
from operator import add 
from pyspark import SparkContext, SparkConf

sparkConf = SparkConf().setAppName("wordCount").setMaster("local")
sc = SparkContext(conf = sparkConf)
texte = sc.textFile("sample.txt")

rdd_counts = texte \
    .flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word,1)) \
    .reduceByKey(add)

counts = rdd_counts.collect()

print(counts)
for (word, count) in counts:
    print(word, count)

rdd_counts.coalesce(1).saveAsTextFile("result")