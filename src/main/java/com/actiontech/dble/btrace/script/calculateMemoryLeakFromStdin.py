#  Copyright (C) 2016-2021 ActionTech.
#  based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
#  License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
import re
import select
import sys
import time


def calculate():
    try:
        address_ = ele['address']
        if address_ not in totalMap:
            totalMap[address_] = ele
            totalMap[address_]['count'] = 0
            totalMap[address_]['createTime'] = time.time()
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


def print_result():
    global lastCheckTime
    # if ele != {}:
    #     calculate()
    #     ele = {}
    currentTime = time.time()
    if currentTime - lastCheckTime <= 8:
        return
    print("ping " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    lastCheckTime = currentTime
    arr = []
    for value in totalMap.values():
        if (time.time() - value['createTime']) > 10 and value['count'] > 0:
            arr.append(value)
    if len(arr) > 0:
        print("-------------------------------------------------------")
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print("may contain memory leak: ")

    connection_count = 0
    for ele in arr:
        is_connections = False
        for stacktrace in ele['stacktrace']:
            if "setProcessor" in stacktrace or 'ensureFreeSpaceOfReadBuffer' in stacktrace or 'onReadData' in stacktrace:
                connection_count += 1
                print(stacktrace)
                is_connections = True
                break
        if not is_connections:
            print(ele)

    if len(arr) > 0:
        print("un release connection count:" + str(connection_count))
        print("-------------------------------------------------------")


lastCheckTime = time.time()
totalMap = {}
lineNum = 0
ele = {}
index = 0
start = False
stacktrace = []
while True:
    try:
        if select.select([sys.stdin, ], [], [], 10.0)[0]:
            print_result()
            line = sys.stdin.next()
        else:
            print_result()
            continue

    except StopIteration:
        print 'EOF!'
        break
    lineNum = lineNum + 1
    if line.startswith(">>>"):
        start = True
        index = 0
        stacktrace = []
        if ele != {}:
            calculate()
            ele = {}
        continue
    elif line.startswith("<<<"):
        start = False
        index = 0
        ele['stacktrace'] = stacktrace
        if ele != {}:
            calculate()
            ele = {}
        print_result()
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
    elif index == 2:
        m = re.match("^.*address=(\d*),", line)
        if m:
            # print(m.group(1))
            ele['address'] = m.group(1)
    else:
        stacktrace.append(line)
