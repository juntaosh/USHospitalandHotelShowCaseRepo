from pyspark import SparkContext
import difflib


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


def remove_malformed_data(data):
    if len(data) != 25 or data[7] != 'US':
        return data, -1
    else:
        return data, 0


def sequenceMatch(data):
    hotel_name = data[0]
    ratio = 0
    matched_hotel = data
    for hotel in hotel_dict.keys():
        if hotel != hotel_name:
            diff = difflib.SequenceMatcher(None, hotel, hotel_name).ratio()
            if diff >= ratio:
                ratio = diff
                hotel_list = [hotel_name, hotel]
                hotel_list.sort()
                matched_hotel = (1, hotel_list)
    if matched_hotel[0] != hotel_name:
        if ratio >= 0.90:
            return matched_hotel
    return 0, data[0]


def deduplicate(data):
    # Removing the shorter name if the data's address matches 80%
    address_a = list(hotel_dict[data[0]])
    address_b = list(hotel_dict[data[1]])
    # Remove categories columns
    del address_a[4]
    del address_b[4]
    ratio = difflib.SequenceMatcher(None, str(address_a), str(address_b)).ratio()
    if ratio > 0.80:
        # Remove extra item in dictionary. Add new ptr connect this two item
        return 1, data
    return 0, data


# program start here
print("\033[1;31mProgram starts!\033[0m")
hotelFilePath = '../../datasets/Datafiniti_Hotel_Reviews_Jun19.csv'
sc = SparkContext('local[*]', 'Hotel_Reviews')
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

hotelsRDD = initialRDD \
    .map(lambda data: (data[0], (data[1][6], data[1][2], data[1][0], data[1][5], data[1][1], data[1][3], data[1][4]))) \
    .distinct()
hotel_dict = hotelsRDD.collectAsMap()
hotel_dup_dict = {}
duplicate_hotels = hotelsRDD.map(lambda data: sequenceMatch(data))\
    .filter(lambda data: data[0] == 1)\
    .map(lambda data: (data[1][0], data[1][1]))\
    .distinct()\
    .map(lambda data: deduplicate(data))
true_positive = duplicate_hotels.filter(lambda data: data[0] == 1)\
    .map(lambda data: data[1])\
    .collect()

false_positive = duplicate_hotels.filter(lambda data: data[0] == 0).count()
print("Running Jaccard similarity on hotel names collected %s lines of data, true positive %s lines, and false postive"
      " %s lines."
      % (duplicate_hotels.count(),
         len(true_positive),
         false_positive))
for dup in true_positive:
    print("Removed duplicate item: %s, duplicate item for: %s." % (
        dup[1],
        dup[0]
    ))
    del hotel_dict[dup[1]]
    hotel_dup_dict[dup[0]] = dup[1]

print("Saving hotel data to file.")
output_file = open("General_Hotel_Data.json", "w")
counter = 0
id_dict = {}
for hotel_name in hotel_dict.keys():
    id_dict[hotel_name] = counter
    counter += 1
counter = 0
output_file.write("{")
output_file.write('"Hotels": {')
for hotel_name in id_dict.keys():
    output_file.write('"' + str(id_dict[hotel_name]) + '": {')
    output_file.write('"Id": ' + str(id_dict[hotel_name]) + ',')
    output_file.write('"Name": ' + '"' + hotel_name + '",')
    output_file.write('"State": ' + '"' + hotel_dict[hotel_name][0] + '",')
    output_file.write('"City": ' + '"' + hotel_dict[hotel_name][1] + '",')
    output_file.write('"Address": ' + '"' + hotel_dict[hotel_name][2] + '",')
    output_file.write('"Zip": ' + '"' + hotel_dict[hotel_name][3] + '",')
    output_file.write('"Type": ' + '"' + hotel_dict[hotel_name][4] + '",')
    output_file.write('"Latitude": ' + hotel_dict[hotel_name][5] + ',')
    output_file.write('"Longitude": ' + hotel_dict[hotel_name][6])
    counter += 1
    if counter != len(id_dict):
        output_file.write('},')
    else:
        output_file.write('}')
output_file.write('}')
output_file.write('}')
output_file.close()
print("Data saved to General_Hotel_Data.json")
