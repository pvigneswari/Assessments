import sys

from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local[1]", "AppName")

# Read the text file into an RDD
path = sc.textFile(str(sys.argv[1]))

# Split each line into words
words = path.flatMap(lambda line: line.split(" "))

# Map each word to a tuple (word, 1)
word_count = words.map(lambda w: (w, 1))

# Count the occurrences of each word
result = word_count.countByKey()

# Print the result
for word, count in result.items():
    # print(f"{word}: {count}")
    print("{}: {}".format(word, count))

# Stop the SparkContext
sc.stop()
