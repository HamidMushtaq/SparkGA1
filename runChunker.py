#!/usr/bin/python
#****************************************/
#	Script:		runChunker.py	
#	Author:		Hamid Mushtaq  		
#	Company:	TU Delft	 	
#****************************************/
from xml.dom import minidom
import sys
import os
import time

if len(sys.argv) < 2:
	print("Not enough arguments!")
	print("Example usage: ./runChunker.py config.xml")
	sys.exit(1)

exeName = "chunker/chunker_2.11-1.0.jar"
configFile = sys.argv[1]
doc = minidom.parse(configFile)
inputFileName = doc.getElementsByTagName("fastq1Path")[0].firstChild.data
fastq2Path = doc.getElementsByTagName("fastq2Path")[0].firstChild
inputFileName2 = "" if (fastq2Path == None) else fastq2Path.data
outputFolder = doc.getElementsByTagName("outputFolder")[0].firstChild.data
driver_mem = doc.getElementsByTagName("driverMemGB")[0].firstChild.data + "g"

def run():
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--class \"hmushtaq.fastqchunker.Chunker\" --master local[*] --driver-memory " + driver_mem + " " + exeName + " " + configFile
	
	print cmdStr
	os.system(cmdStr)

start_time = time.time()
os.system("hadoop fs -rm -r -skipTrash " + outputFolder)
run()

time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60
print "||| Time taken for chunker = " + str(mins) + " mins " + str(secs) + " secs |||"
