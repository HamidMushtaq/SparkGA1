#!/usr/bin/python
#****************************************/
#	Script:		runPart.py	
#	Author:		Hamid Mushtaq  		
#	Company:	TU Delft	 	
#****************************************/
from xml.dom import minidom
import sys
import os
import time
import subprocess
import math
import glob

if len(sys.argv) < 3:
	print("Not enough arguments!")
	print("Example usage: ./runPart.py config.xml 1")
	sys.exit(1)

exeName = "program/sparkga1_2.11-1.0.jar"
logFile = "time.txt"
configFilePath = sys.argv[1]
partNumber = sys.argv[2]

if not os.path.isfile(configFilePath):
	print("Config file " + configFilePath + " does not exist!")
	sys.exit(1)

doc = minidom.parse(configFilePath)
mode = doc.getElementsByTagName("mode")[0].firstChild.data
refPath = doc.getElementsByTagName("refPath")[0].firstChild.data
inputFolder = doc.getElementsByTagName("inputFolder")[0].firstChild.data
outputFolder = doc.getElementsByTagName("outputFolder")[0].firstChild.data
tmpFolder = doc.getElementsByTagName("tmpFolder")[0].firstChild.data
toolsFolder = doc.getElementsByTagName("toolsFolder")[0].firstChild.data
# Parameters for this part
numTasks = doc.getElementsByTagName("numTasks" + partNumber)[0].firstChild.data
if mode != 'local':
	numInstances = doc.getElementsByTagName("numInstances" + partNumber)[0].firstChild.data
	exe_mem = doc.getElementsByTagName("execMemGB" + partNumber)[0].firstChild.data + "g"
driver_mem = doc.getElementsByTagName("driverMemGB" + partNumber)[0].firstChild.data + "g"

print "mode = [" + mode + "]"

def getNumOfHadoopChunks():
	cat = subprocess.Popen(["hadoop", "fs", "-ls", inputFolder], stdout=subprocess.PIPE)
	count = 0.0
	for line in cat.stdout:
		if inputFolder in line:
			count = count + 1.0
	return count
	
def getNumOfLocalChunks():
	files = glob.glob(inputFolder + "/*.gz")
	return float(len(files))

def executeHadoop(part, ni, em):	
	dictHDFSPath = refPath.replace(".fasta", ".dict")
	dictPath = './' + dictHDFSPath[dictHDFSPath.rfind('/') + 1:]
			
	tools = glob.glob(toolsFolder + '/*')
	toolsStr = ''
	for t in tools:
		toolsStr = toolsStr + t + ','
	toolsStr = toolsStr[0:-1]
	
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--jars lib/htsjdk-1.143.jar " + \
	"--class \"hmushtaq.sparkga1.SparkGA1\" --master " + mode + " " + \
	"--files " + configFilePath + "," + dictPath + "," + toolsStr + " " + \
	"--driver-memory " + driver_mem + " --executor-memory " + em + " " + \
	"--num-executors " + ni + " --executor-cores " + numTasks + " " + \
	exeName + " " + configFilePath + " " + str(part)
	
	print cmdStr
	addToLog("[" + time.ctime() + "] " + cmdStr)
	os.system(cmdStr)
	
def executeLocal(part):
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--jars lib/htsjdk-1.143.jar " + \
	"--class \"hmushtaq.sparkga1.SparkGA1\" --master local[" + numTasks + "] --driver-memory " + driver_mem + " " + exeName + " " + \
	configFilePath + " " + str(part)
	
	print cmdStr
	addToLog("[" + time.ctime() + "] " + cmdStr)   
	os.system(cmdStr)
	
def addToLog(s):
	f = open(logFile,'a+')
	f.write(s + "\n")
	f.close() 

def runHadoopMode(part):
	if part == 1:
		addToLog("########################################\n[" + time.ctime() + "] Part1 started.")
		dictHDFSPath = refPath.replace(".fasta", ".dict")
		dictPath = './' + dictHDFSPath[dictHDFSPath.rfind('/') + 1:]
		if not os.path.exists(dictPath):
			os.system("hadoop fs -get " + dictHDFSPath)
		os.system("hadoop fs -rm -r -f " + outputFolder)
	
	executeHadoop(part, numInstances, exe_mem)
	addToLog("[" + time.ctime() + "] Part" + str(part) + " completed.")
	
def runLocalMode(part):
	if part == 1:
		addToLog("[" + time.ctime() + "] Part1 started.")
		if os.path.isdir(outputFolder):
			os.system("rm -r -f " + outputFolder)
		os.system("mkdir " + outputFolder)
		if os.path.isdir(tmpFolder):
			os.system("rm -r -f " + tmpFolder)
		os.system("mkdir " + tmpFolder)
		
	executeLocal(part)
	addToLog("[" + time.ctime() + "] Part" + str(part) + " completed.")
	if part == 3:
		if not os.path.exists(outputFolder + '/vcfs'):
			os.makedirs(outputFolder + '/vcfs')
		os.system('mv ' + tmpFolder + '/sparkCombined.vcf ' + outputFolder)
		os.system('mv ' + tmpFolder + '/*.vcf ' + outputFolder + '/vcfs')
	
start_time = time.time()

if (mode == "local"):
	runLocalMode(int(partNumber))
else:
	runHadoopMode(int(partNumber))
	
time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60

print "|| Time taken = " + str(mins) + " mins " + str(secs) + " secs ||"
