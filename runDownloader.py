#!/usr/bin/python
#****************************************/
#	Script:		runDownloader.py	
#	Author:		Hamid Mushtaq  		
#	Company:	TU Delft	 	
#****************************************/
from xml.dom import minidom
import sys
import os
import time

# Options:
# 	cpAll: Copy all the reference and tool files to the tmp directory of all nodes
# 	rmAll: Remove all files from the tmp directory of all nodes
# 	rmExt: Remove files of a certain extension from the tmp directory of all nodes
# 	ls: Display files in tmp directory of all nodes
# 	cp: Copy a file in the tools directory to the tmp directory of all nodes

exeName = "filesDownloader/filesdownloader_2.10-1.0.jar"

if len(sys.argv) < 3:
	print "Too few arguments"
	print "Usage: python runDownloader.py configFile mode [extra]" 
	print "Example: python runDownloader.py config.xml cpAll" 
	sys.exit(1)
	
configFilePath = sys.argv[1]

if not os.path.isfile(configFilePath):
	print("Config file " + configFilePath + " does not exist!")
	sys.exit(1)
	
mode = sys.argv[2]
third_arg = ""
if len(sys.argv) == 4:
	third_arg = sys.argv[3]

doc = minidom.parse(configFilePath)
numNodes = doc.getElementsByTagName("numNodes")[0].firstChild.data
nodeMemoryGB = doc.getElementsByTagName("nodeMemoryGB")[0].firstChild.data
usedMemoryGB = str(int(nodeMemoryGB) * 80 / 100)
	
start_time = time.time()

cmdStr = "$SPARK_HOME/bin/spark-submit " + \
"--class \"FilesDownloader\" --master yarn-cluster --files " + configFilePath + " " + \
"--driver-memory 8g --executor-memory " + usedMemoryGB + "g --num-executors " + numNodes + " " + \
exeName + " " + os.path.basename(configFilePath) + " " + mode + " " + third_arg
	
print cmdStr
os.system(cmdStr)

time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60

os.system("hadoop fs -cat sparkLog.txt")

print "<< Time taken = " + str(mins) + " mins " + str(secs) + " secs >>"	