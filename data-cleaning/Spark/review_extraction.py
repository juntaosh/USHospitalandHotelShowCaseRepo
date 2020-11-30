from pyspark import SparkContext
import re
import math


# Split lines into words, not using regex to save performance
def splitWords(line):
    words = ''
    neglect = False
    result = []
    for char in line:
        if char == ',' and not neglect and words != '':
            result.append(words)
            words = ''
        else:
            if char == '"':
                neglect = not neglect
            else:
                words += char
    if words != '':
        result.append(words)
    return result


# Removes data that has nulls and also header
def remove_malformed_data(data):
    if len(data) != 25 or data[7] != 'US':
        return data, -1
    else:
        return data, 0


def reassign_id(review):
    return (review_dict[review], (review[0], review[1][0], review[1][1], review[1][2], review[1][3]))


# We need to tokennize the reviews by tokenize each words, and remove stop words.
def tokenizeReview(review_data):
    return review_data[0], list(filter(lambda r: r.lower() not in stopwords, tokenize(review_data[1][3])))


def tokenize(review_data):
    return list(filter(lambda r: len(r) > 0, re.split(r'\W+', review_data.lower())))


# TF - term frequency
def term_frequency(tokens):
    token_counts = {}
    for token in tokens:
        if token not in token_counts:
            token_counts[token] = 1
        else:
            token_counts[token] += 1
    total_tokens = len(tokens)
    term_frequency = {}
    for token in token_counts.keys():
        term_frequency[token] = 1.00 * token_counts[token]/ total_tokens
    return term_frequency


# IDF - Inverse Document Frequency
def inverse_document_frequency(rdd):
    tokensRDD = rdd.flatMap(lambda review: list(set(review[1]))).map(lambda t: (t,1)).reduceByKey(lambda a,b:a+b)
    return (tokensRDD.map(lambda t: (t[0], float(total_document_count/t[1]))))


# TF-IDF Term Frequency - Inverse Document Frequency
def tf_idf(review_data, idfMapBroadCasted):
    tfs = term_frequency(review_data)
    tfidf = {}
    for token in tfs.keys():
        tfidf[token] = tfs[token] * idfMapBroadCasted.value[token]
    return tfidf


# Compute cossine similarity between review based on their TF-IDF scores on duplicate tokens
def dotProduct(review_a, review_b):
    return sum([review_a[t] * review_b[t] for t in review_a if t in review_b])


def norm(review_a):
    return math.sqrt(dotProduct(review_a, review_a))


def cossine(review_a, review_b):
    return dotProduct(review_a, review_b) /(norm(review_a) * norm(review_b))
print(cossine({"a": 0.5, "b": 0.2}, {"a": 0.5, "b": 0.2, "c": 0.2}))


# Compare each review with each other review:
def compare(review_data, reviewTFIDFMapBroadCast):
    duplicate_review_ids = []
    for review_id in reviewTFIDFMapBroadCast.value.keys():
        if review_data[0] >= review_id:
            continue
        else:
            similarity = cossine(review_data[1], reviewTFIDFMapBroadCast.value[review_id])
            if similarity >= 0.95:
                duplicate_review_ids.append(review_id)
    return review_data[0], duplicate_review_ids


# program start here
print("\033[1;31mProgram starts!\033[0m")
hotelFilePath = '../../datasets/Datafiniti_Hotel_Reviews_Jun19.csv'
sc = SparkContext('local[*]', 'Review_extraction')
sc.setLogLevel("ERROR")

hotelFile = sc.textFile(hotelFilePath)
initialRDD = hotelFile.flatMap(lambda line: line.split('\n')) \
    .map(lambda line: splitWords(line)) \
    .map(lambda data: remove_malformed_data(data)) \
    .filter(lambda data: data[1] == 0) \
    .map(lambda data: data[0]) \
    .map(lambda line: (line[11], [line[3], line[4], line[6], line[9],
                                  line[10], line[12], line[13], line[16], line[18], line[19], line[0], line[14][0:11]]))
print("Initial cleaning saved %s lines of data."
      % (initialRDD.count()))

# Collect only the review portion of all data and uses distinct to remove duplicate reviews
reviewRDD = initialRDD \
    .map(lambda data: (data[1][10], (data[0], data[1][7], data[1][8], data[1][9]))) \
    .distinct()
total_document_count = reviewRDD.count()
print("Total review count before cleaning has %s reviews." % (total_document_count))
print(reviewRDD.take(1))

# Since we have duplicate review_ID in this dataset, we need to re-assign their ids.
reviewList = reviewRDD.collect()
review_dict = {}
counter = 0
for review in reviewList:
    review_dict[review] = counter
    counter += 1
reviewRDD = reviewRDD.map(reassign_id)
print(reviewRDD.take(1))

# We need to remove stopwords to compute the real similarity between reviews. Using set to remove duplicates and
# for faster comparison
stopwords = set(sc.textFile('../../datasets/stopwords.txt').flatMap(lambda data: data.split('\n')).collect())

# Tokenize every review
reviewTokenized = reviewRDD.map(tokenizeReview)
print(reviewTokenized.take(1))

# Calculate the IDF for each token in the review
idfMap = inverse_document_frequency(reviewTokenized).collectAsMap()
idfMapBroadCast = sc.broadcast(idfMap)

# Perform TFIDF on each review
reviewTFIDF = reviewTokenized.map(lambda review_data: (review_data[0], tf_idf(review_data[1], idfMapBroadCast)))
print(reviewTFIDF.take(1))

# Collect the review TFIDF as a map:
reviewTFIDFMap = reviewTFIDF.collectAsMap()
reviewTFIDFMapBroadCast = sc.broadcast(reviewTFIDFMap)
print(reviewTFIDFMapBroadCast.value[0])

possibleDuplicateReviewMap = reviewTFIDF.map(lambda review_data: compare(review_data, reviewTFIDFMapBroadCast)).collectAsMap()
duplicateCandidates = []
for review_id in possibleDuplicateReviewMap.keys():
    if len(possibleDuplicateReviewMap[review_id]) > 0:
        duplicateCandidates[review_id] = possibleDuplicateReviewMap[review_id]
reviewMap = reviewRDD.collectAsMap()
for duplicateKey in duplicateCandidates.keys():
    for k in duplicateCandidates[duplicateKey]:
        if k in reviewMap.keys():
            del reviewMap[k]
print("Saving review data to file.")
output_file = open("General_Review_Data.json", "w")
counter = 0
output_file.write("{")
output_file.write('"Reviews": {')
for review_id in reviewMap.keys():
    output_file.write('"' + str(review_id) + '": {')
    output_file.write('"Id": ' + str(review_id) + ',')
    output_file.write('"ReviewId": ' + '"' + reviewMap[review_id][0] + '",')
    output_file.write('"ReviewEdHotel": ' + '"' + reviewMap[review_id][1] + '",')
    output_file.write('"ReviewedScore": ' + '"' + reviewMap[review_id][2] + '",')
    output_file.write('"ReviewedText": ' + '"' + reviewMap[review_id][3] + '",')
    output_file.write('"ReviewedTitle": ' + '"' + reviewMap[review_id][4] + '"')
    counter += 1
    if counter != len(reviewMap):
        output_file.write('},')
    else:
        output_file.write('}')
output_file.write('}')
output_file.write('}')
output_file.close()
print("Data saved to General_Review_Data.json")