import MapReduce
import sys
import re
import json

# Part 1
mr = MapReduce.MapReduce()
json.JSONEncoder.key_separator = ','

# Part 2
def mapper(record):
    # key: document identifier
    # value: document contents
    # key = record[0]
    # value = record[1]
    # words = value.split()
    # for w in words:
    #     mr.emit_intermediate(w, 1)
    key = record[0].lower()
    value = record[1].lower()
    words = value.split()
    for w in words:
        if re.match((r'[A-Za-z0-9]+'), w):
            occur = len(re.findall(r"" + re.escape(w), re.escape(value)))
            mr.emit_intermediate(w, (key, occur))


# Part 3
def reducer(key, list_of_values):
    total = 0
    # list_of_values = list(set(list_of_values))
    x = []
    for v in list_of_values:
        if v not in x:
            x.append(v)
            total += 1
    mr.emit((key, total, x))


# Part 4
#words file is passed as an argument here
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
