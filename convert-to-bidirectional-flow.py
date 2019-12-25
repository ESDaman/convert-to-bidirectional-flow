#!/usr/bin/bash
import os
import sys
import glob
from datetime import timedelta, date

#
##
###
#### By Evan Daman
#### 
#### CAO: 20191106
###
##
#

try:
    uFile = sys.argv[1]
except:
    print("\nconvert-to-bidirectional-flow.py combines uni-directional SILK flows into bi-directional flows for easier analysis")
    print("Usage: python convert-to-bidirectional-flow.py inputfile.txt\n")
    print("\nPlease specify a text file containing ascii silk flow records")
    print("Your pwd must be the location of the input file")
    print("\nIMPORTANT: For the most accurate results follow these commands to make your txt file.\n")
    print("rwsort --fields=9 <flowrecord.bin> | rwcut >> <silkflows.txt>")
    print("uniq <silkflows.txt> >> <filename.txt>")
    print("\nTIP: prefix your command with LC_ALL=C to improve speed")
    quit()

if (uFile == "-h" or uFile == "--help"):
    print("\nconvert-to-bidirectional-flows.py combines uni-directional SILK flows into bi-directional flows for easier analysis")
    print("Usage: python convert-to-bidirectional-flow.py inputfile.txt\n")
    print("\nPlease specify a text file containing ascii silk flow records")
    print("Your pwd must be the location of the input file")
    print("\nIMPORTANT: For the most accurate results follow these commands to make your txt file.\n")
    print("rwsort --fields=9 <flowrecord.bin> | rwcut >> <silkflows.txt>")
    print("uniq <silkflows.txt> >> <filename.txt>")
    print("\nTIP: prefix your command with LC_ALL=C to improve speed")
    quit()

if not os.path.isfile(sys.argv[1]):
    print(uFile + " does not exist.")
    print("\nconvert-to-bidirectional-flows.py combines uni-directional SILK flows into bi-directional flows for easier analysis")
    print("Usage: python convert-to-bidirectional-flow.py inputfile.txt\n")
    print("\nPlease specify a text file containing ascii silk flow records")
    print("Your pwd must be the location of the input file")
    print("\nIMPORTANT: For the most accurate results follow these commands to make your txt file.\n")
    print("rwsort --fields=9 <flowrecord.bin> | rwcut >> <silkflows.txt>")
    print("uniq <silkflows.txt> >> <filename.txt>")
    print("\nTIP: prefix your command with LC_ALL=C to improve speed")
    quit()

print("\nIf the script encounters an error. Please read the help statement\n")


#Function to iterate through date for use in filename
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)



#Open user supplied text flow
uFlow = open(uFile)
uLine = uFlow.readlines()

dateArray = []

for line in uLine:
    line = line.replace(" ","")
    line = line.replace("|",",")
    line = line.rstrip()
    dateTemp = line.split(",")
    dateVal = dateTemp[10].split("T")
    if dateVal[0] not in dateArray:
        dateArray.append(dateVal[0])
dateArray.sort(reverse=True)


date2 = dateArray[1]
date1 = dateArray[-1]
print("Start date: " + str(date1))
print("End date: " + str(date2))


date1 = date1.split("/")
date2 = date2.split("/")


#Init time scope
start_date = date(int(date1[0]), int(date1[1]), int(date1[2]))
end_date = date(int(date2[0]), int(date2[1]), int(date2[2]) + 2)


for single_date in daterange(start_date, end_date):
    fileList = glob.glob(single_date.strftime("%Y-%m-%d") + "-" + uFile + "-*")
    for filepath in fileList:
        if os.path.isfile(filepath):
            os.remove(filepath)
fileList = []


fNames = []


#Break out Input file by day
for single_date in daterange(start_date, end_date):
    #Print date to show progress
    print(single_date.strftime("%Y-%m-%d"))
    f = 0
    i = 1
    for line in uLine:
        line = line.replace(" ", "")
        line = line.replace("|", ",")
        line = line.rstrip()
        lineArrayDay = line.split(",")


        #If our date is in the line
        if single_date.strftime("%Y/%m/%d") in lineArrayDay[10]:
            if i == 10000:
                f = f + 1
                i = 1
            if i < 10000:
                fileName = single_date.strftime("%Y-%m-%d") + "-" + uFile + "-" + str(f)
                open(fileName, "a").write(line + "\n")
                i = i + 1
                if fileName not in fNames:
                    fNames.append(fileName)


#Open the final result file
results = open("Bidirectional-" + uFile, "a")
results.write("src" + "," + "sport" + "," + "dest" + "," + "dport" + "," + "bytes S->D" + "," + "bytes S<-D" + "," + "bytesTotal" + "," + "packets S->D" + "," + "packets S<-D" + "," + "packetTotal" + "," + "srcFlags" + "," + "destFlags" + "," + "startTime" + "," + "endTime" + "\n")



#Create Bi-Directional flow records
for fName in fNames:
    #Display script progress
    print(fName)
    if os.path.isfile(fName):
        dayFile = open(fName, "a+")
        dayLine = dayFile.readlines()

        #Initialize our list that will hold previous positive hits
        skipList = []
        
        #Init a skip var so we can keep going through the file until all lines have been added to skipList
        b = 0
        previousPos = -1
        i = 0
        r = 1

        while (i < len(dayLine) and b == 0):
            if (r == 1):
                src = "src"
                sport = 0
                dest = "dest"
                dport = 0
                bytesAB = 0
                bytesBA = 0
                bytesTotal = 0
                packetAB = 0
                packetBA = 0
                sFlags = "null"
                dFlags = "null"
                packetTotal = 0
                endTime = "null"
                startTime = "null"
                r = 0
                s = 0
                b = 0
                o = 0


#0-src
#1-dest
#2-sport
#3-dport
#5-packets
#6-bytes
#7-flags
#8-startTime
#10-endTime


            if dayLine[i] not in skipList:
                lineArray = dayLine[i].split(",")
                if (s == 0):
                    s = 1
                    src = lineArray[0]
                    sport = int(lineArray[2])
                    startTime = lineArray[8]
                    sFlags = lineArray[7]
                    endTime = lineArray[10]
                    bytesAB = bytesAB + int(lineArray[6])
                    bytesTotal = bytesTotal + int(lineArray[6])
                    packetAB = packetAB + int(lineArray[5])
                    packetTotal = packetTotal + int(lineArray[5])
                    dest = lineArray[1]
                    dport = lineArray[3]
                    otherSide = dest + "," + src + "," + str(dport) + "," + str(sport)
                    previousPos = i

                if (otherSide in dayLine[i]):
                    skipList.append(dayLine[i])
                    packetBA = packetBA + int(lineArray[5])
                    packetTotal = packetTotal + int(lineArray[5])
                    bytesBA = bytesBA + int(lineArray[6])
                    bytesTotal = bytesTotal + int(lineArray[6])
                    endTime = lineArray[10]
                    dFlags = lineArray[7]
                    o = 1
                    r = 1

            #If we've seen a src/dest write immediatly and reset
            #If we've seen only a src this whole file(no dest flow). write and reset.
            if (s == 1 and ((o == 1) or (dayLine[i] == dayLine[-1]))):
                results.write(str(src) + "," + str(sport) + "," + str(dest) + "," + str(dport) + "," + str(bytesAB) + "," + str(bytesBA) + "," + str(bytesTotal) + "," + str(packetAB) + "," + str(packetBA) + "," + str(packetTotal) + "," + str(sFlags) + "," + str(dFlags) + "," + str(startTime) + "," + str(endTime) + "\n")
                r = 1
                i = previousPos

            #Break out of this day's file if no new lines are encountered and a line hasnt been written this loop
            if ((s == 0) and (r == 0) and dayLine[i] == dayLine[-1]):
                b = 1

            i = i + 1




dayFile.close()
for fName in fNames:
    os.remove(fName)
if os.path.isfile("Bidirectional-" + uFile):
    print("File Bidirectional-" + uFile + " created!")
