import pyspark
import sys
import nltk

stop_words = set(nltk.corpus.stopwords.words('english'))
tokenizer = nltk.tokenize.RegexpTokenizer(r'\w+')


def stop_words_filter(line):
    text = tokenizer.tokenize(line)
    words = []
    for word in text:
        word = word.lower()
        if word not in stop_words:
            words.append(word)
    return words


if __name__ == '__main__':
    inputUri = sys.argv[1]

    sc = pyspark.SparkContext()
    lines = sc.textFile(sys.argv[1])
    words = lines.flatMap(stop_words_filter)
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2).sortBy(
        lambda x: x[1], False)
    res = wordCounts.take(5)
    print(res)
