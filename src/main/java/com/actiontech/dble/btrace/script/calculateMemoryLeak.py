#  Copyright (C) 2016-2021 ActionTech.
#  based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
#  License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
import re
import sys

totalMap = {}
if len(sys.argv) != 2:
    print("must specify a filename")
    exit(1)
fileName = sys.argv[1]
print("read from: " + fileName)

lineNum = 0


def calculate():
    try:
        address_ = ele['address']
        if address_ not in totalMap:
            totalMap[address_] = ele
            totalMap[address_]['count'] = 0

        if ele['type'] == 'allocate':
            totalMap[address_]['allocateTime'] = ele['allocateTime']
            totalMap[address_]['count'] = totalMap[address_]['count'] + 1
        else:
            totalMap[address_]['recycleTime'] = ele['recycleTime']
            totalMap[address_]['count'] = totalMap[address_]['count'] - 1
        if totalMap[address_]['count'] == 0:
            del totalMap[address_]
    except KeyError:
        print("illegal line " + str(lineNum))


with open(fileName) as fp:
    ele = {}
    index = 0
    start = False
    for line in fp:
        lineNum = lineNum + 1
        if line.startswith(">>>"):
            start = True
            index = 0
            if ele != {}:
                calculate()
                ele = {}
            continue
        elif line.startswith("<<<"):
            start = False
            index = 0
            if ele != {}:
                calculate()
                ele = {}
            continue

        index = index + 1
        if not start:
            continue
        if index == 1:
            m = re.match("^.*type:\"(.*)\",time:\"(.*)\"", line)
            if m:
                ele['type'] = m.group(1)
                if ele['type'] == 'allocate':
                    ele['allocateTime'] = m.group(2)
                else:
                    ele['recycleTime'] = m.group(2)
            # print(m.group(1))
            # print(m.group(2))
            else:
                print ("unrecognized line: " + line)
        else:
            m = re.match("^.*address=(\d*),", line)
            if m:
                # print(m.group(1))
                ele['address'] = m.group(1)

    # if ele != {}:
    #     calculate()
    #     ele = {}
if len(totalMap) > 0:
    print("may contain memory leak: ")
for value in totalMap.values():
    print(value)
