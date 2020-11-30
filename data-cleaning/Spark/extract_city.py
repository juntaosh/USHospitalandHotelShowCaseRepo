f = open("US.txt", "r")
result = ""
for line in f:
    data = line.split("\t")
    result += data[2] + " "
result = result[:-1]
of = open("us_city.txt", "w")
of.write(result)
f.close()
of.close()