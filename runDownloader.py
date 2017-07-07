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
# 	cpAll: Copy all the reference and index files to the local directory (sfFolder) of all nodes
# 	cp: Copy a file from the input hdfs folder to the local directory (sfFolder) of all nodes
# 	rmAll: Remove all files from the local directory (sfFolder) of all nodes
# 	rmJunk: Remove junk files, that is, all files besides the reference and index files, from the local directory (sfFolder) of all nodes
# 	rmExt: Remove files of a certain extension from the local directory directory (sfFolder) of all nodes
#	rm: Remove a file from the local directory (sfFolder) of all nodes
# 	ls: Display files in the local directory (sfFolder) of all nodes

exeName = "filesDownloader/filesdownloader_2.11-1.0.jar"

if len(sys.argv) < 3:
	print "Too few arguments"
	print "Usage: python runDownloader.py config.xml mode [extra]" 
	print "Example: python runDownloader.py config.xml cpAll" 
	sys.exit(1)
	
configFilePath = sys.argv[1]

if not os.path.isfile(configFilePath):
	print("Config file " + configFilePath + " does not exist!")
	sys.exit(1)
	
mode = sys.argv[2]
if (len(sys.argv) < 4) and ((mode == "rm") or (mode == "rmExt") or (mode == "cp")):
	print "Extra parameter missing for " + mode
	sys.exit(1)
third_arg = ""
if len(sys.argv) == 4:
	third_arg = sys.argv[3]

doc = minidom.parse(configFilePath)
deploy_mode = doc.getElementsByTagName("mode")[0].firstChild.data
numNodes = doc.getElementsByTagName("numNodes")[0].firstChild.data
exe_mem = doc.getElementsByTagName("execMemGB")[0].firstChild.data + "g"
driver_mem = doc.getElementsByTagName("driverMemGB")[0].firstChild.data + "g"

start_time = time.time()

cmdStr = "$SPARK_HOME/bin/spark-submit " + \
"--class \"FilesDownloader\" --master " + deploy_mode + " --files " + configFilePath + " " + \
"--driver-memory " + driver_mem + " --executor-memory " + exe_mem + " --num-executors " + numNodes + " " + \
exeName + " " + configFilePath + " " + mode + " " + third_arg

print cmdStr
os.system(cmdStr)

time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60

os.system("hadoop fs -cat sparkLog.txt")

print "<< Time taken = " + str(mins) + " mins " + str(secs) + " secs >>"	