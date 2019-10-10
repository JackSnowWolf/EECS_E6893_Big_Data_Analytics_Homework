import pyspark
import sys

if __name__ == '__main__':
    inputUri = sys.argv[1]

    sc = pyspark.SparkContext()
    lines = sc.textFile(sys.argv[1])
    words = lines.flatMap(lambda line: line.split())
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2).sortBy(
        lambda x: x[1], False)
    res = wordCounts.take(5)
    print(res)
