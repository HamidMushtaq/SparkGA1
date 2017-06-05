/*
 * Copyright (C) 2016-2017 Hamid Mushtaq, TU Delft
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.commons.io.FileUtils
import sys.process._

import java.io._
import java.net._

import tudelft.utils.Configuration
import tudelft.utils.HDFSManager

object FilesDownloader 
{
//////////////////////////////////////////////////////////////////////////////
def downloadFiles (n: Integer, config: Configuration) : (String, String, Integer) = 
{
	val IP = InetAddress.getLocalHost()
	var r = 0
	
	new File(config.getTmpFolder()).mkdirs()
	
	val refFolder = getDirFromPath(config.getRefPath())
	val refFileName = getFileNameFromPath(config.getRefPath())
	r = r + HDFSManager.download(config.getHadoopInstall, refFileName, refFolder, config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, refFileName.replace(".fasta", ".dict"), refFolder, config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, refFileName + ".amb", refFolder, config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, refFileName + ".ann", refFolder, config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, refFileName + ".bwt", refFolder, config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, refFileName + ".fai", refFolder, config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, refFileName + ".pac", refFolder, config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, refFileName + ".sa", refFolder, config.getTmpFolder())
	
	val snpFolder = getDirFromPath(config.getSnpPath())
	val snpFileName = getFileNameFromPath(config.getSnpPath())
	r = r + HDFSManager.download(config.getHadoopInstall, snpFileName, snpFolder, config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, snpFileName + ".idx", snpFolder, config.getTmpFolder())
	
	val indelFolder = getDirFromPath(config.getIndelPath())
	val indelFileName = getFileNameFromPath(config.getIndelPath())
	r = r + HDFSManager.download(config.getHadoopInstall, indelFileName, indelFolder, config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, indelFileName + ".idx", indelFolder, config.getTmpFolder())
	
	val exomeFolder = getDirFromPath(config.getExomePath())
	val exomeFileName = getFileNameFromPath(config.getExomePath())
	if (exomeFileName != "")
		r = r + HDFSManager.download(config.getHadoopInstall, exomeFileName, exomeFolder, config.getTmpFolder())
	
	r = r + HDFSManager.download(config.getHadoopInstall, "bwa", config.getToolsFolder(), config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, "bedtools", config.getToolsFolder(), config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, "AddOrReplaceReadGroups.jar", config.getToolsFolder(), config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, "BuildBamIndex.jar", config.getToolsFolder(), config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, "CleanSam.jar", config.getToolsFolder(), config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, "GenomeAnalysisTK.jar", config.getToolsFolder(), config.getTmpFolder())
	r = r + HDFSManager.download(config.getHadoopInstall, "MarkDuplicates.jar", config.getToolsFolder(), config.getTmpFolder())
	
	return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getTmpFolder(), false), r)
}

def copyFile (n: Integer, fname: String, config: Configuration) : (String, String, Integer) = 
{
	val IP = InetAddress.getLocalHost()
	var r = 0
	
	r = r + HDFSManager.download(config.getHadoopInstall, fname, config.getToolsFolder(), config.getTmpFolder())
	
	return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getTmpFolder(), false), r)
}

def deleteFiles (n: Integer, config: Configuration) : (String, String, Integer) = 
{
	val IP = InetAddress.getLocalHost()
	val d = new File(config.getTmpFolder())
	
	//if (config.getTmpFolder() == "/tmp/")
	//	return (IP.toString() + ":t" + n, "Will not delete the /tmp folder!", 0)
	
	if (d.exists && d.isDirectory) 
	{
		val list: List[File] = d.listFiles.filter(_.isFile).toList
		var i = 0
		var r = 0
			
		for(i <- 0 until list.size)
		{
			val filePath = config.getTmpFolder() + list(i).getName()
			new File(filePath).delete()
			val f = new File(filePath)
			if (!f.exists())
				r = r + 1
		}
			
		return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getTmpFolder(), false), r)
	} 
	else
		return (IP.toString() + ":t" + n, "Directory " + config.getTmpFolder() + " doesn't exist", 0)
}

def deleteDir(n: Integer, config: Configuration) : (String, String, Integer) = 
{
	val IP = InetAddress.getLocalHost()
	val d = new File(config.getTmpFolder)
	
	//if (config.getTmpFolder == "/tmp/")
	//	return (IP.toString() + ":t" + n, "Will not delete the /tmp folder!", 0)
	
	if (d.exists && d.isDirectory) 
	{
		FileUtils.deleteDirectory(d)
			
		return (IP.toString() + ":t" + n, "Directory " + config.getTmpFolder + " deleted!", 1)
	} 
	else
		return (IP.toString() + ":t" + n, "Directory " + config.getTmpFolder + " doesn't exist", 0)
}

def isJunkFile (fileName: String, config: Configuration) : Boolean = 
{
	val refFolder = getDirFromPath(config.getRefPath())
	val refFileName = getFileNameFromPath(config.getRefPath())
	if (fileName.contains(refFileName.split('.')(0)))
		return false
	
	val snpFolder = getDirFromPath(config.getSnpPath())
	val snpFileName = getFileNameFromPath(config.getSnpPath())
	if (fileName.contains(snpFileName))
		return false
	
	val indelFolder = getDirFromPath(config.getIndelPath())
	val indelFileName = getFileNameFromPath(config.getIndelPath())
	if (fileName.contains(indelFileName))
		return false
	
	val exomeFolder = getDirFromPath(config.getExomePath())
	val exomeFileName = getFileNameFromPath(config.getExomePath())
	if ((exomeFileName != "") && fileName.contains(exomeFileName))
		return false
	
	if((fileName == "bwa") || (fileName == "bedtools") || (fileName.contains(".jar"))) 
		return false
	
	return true
}

def deleteJunkFiles (n: Integer, config: Configuration) : (String, String, Integer) = 
{
	val IP = InetAddress.getLocalHost()
	val d = new File(config.getTmpFolder())
	
	//if (config.getTmpFolder() == "/tmp/")
	//	return (IP.toString() + ":t" + n, "Will not delete the /tmp folder!", 0)
	
	if (d.exists && d.isDirectory) 
	{
		val list: List[File] = d.listFiles.filter(_.isFile).toList
		var i = 0
		var r = 0
			
		for(i <- 0 until list.size)
		{
			if (isJunkFile(list(i).getName, config))
			{
				val filePath = config.getTmpFolder + list(i).getName
				new File(filePath).delete()
				val f = new File(filePath)
				if (!f.exists)
					r = r + 1
			}
		}
			
		return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getTmpFolder(), false), r)
	} 
	else
		return (IP.toString() + ":t" + n, "Directory " + config.getTmpFolder() + " doesn't exist", 0)
}

def deleteSubFolders (n: Integer, config: Configuration) : (String, String, Integer) = 
{
	val IP = InetAddress.getLocalHost()
	val d = new File(config.getTmpFolder())
	
	if (d.exists && d.isDirectory) 
	{
		val list: List[File] = d.listFiles.filter(_.isDirectory).toList
		val s = new StringBuilder()
		
		for(i <- 0 until list.size)
		{
			if (!(config.getTmpFolder == "/tmp/") || list(i).getName.contains("jvm") ||
				list(i).getName.contains("yarn") || list(i).getName.contains("hadoop"))
			{	
				s.append("deleting folder " + list(i).getName() + "\n")
				FileUtils.deleteDirectory(list(i))
			}
		}
		
		return (IP.toString() + ":t" + n, s.toString(), 1)
	} 
	else
		return (IP.toString() + ":t" + n, "Directory " + config.getTmpFolder() + " doesn't exist", 0)
}

def deleteExtension (n: Integer, ext: String, config: Configuration) : (String, String, Integer) = 
{
	val IP = InetAddress.getLocalHost()
	val d = new File(config.getTmpFolder())
	
	//if (config.getTmpFolder() == "/tmp/")
	//	return (IP.toString() + ":t" + n, "Will not delete the /tmp folder!", 0)
	
	if (d.exists && d.isDirectory) 
	{
		val list: List[File] = d.listFiles.filter(_.isFile).toList
		var i = 0
		var r = 0
			
		for(i <- 0 until list.size)
		{
			val filePath = config.getTmpFolder() + list(i).getName()
			val index = filePath.indexOf("." + ext)
			if (index != -1)
			{
				new File(filePath).delete()
				val f = new File(filePath)
				if (!f.exists())
					r = r + 1
			}
		}
			
		return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getTmpFolder(), false), r)
	} 
	else
		return (IP.toString() + ":t" + n, "Directory " + config.getTmpFolder() + " doesn't exist", 0)
}

def folderSize(directory: File) : Long =
{
    var length: Long = 0
	for (file <- directory.listFiles) 
	{
        if (file.isFile)
            length += file.length
        else
            length += folderSize(file)
    }
    return length
}

def listDirs(n: Integer, config: Configuration) : (String, String, Integer) =
{
	val IP = InetAddress.getLocalHost()
	val d = new File(config.getTmpFolder())
	
	if (d.exists && d.isDirectory)
	{
		val l = d.listFiles.filter(_.isDirectory)
		val s = new StringBuilder
		
		for(dir <- l)
		{
			val dirSize = folderSize(dir)
			s.append(dir.getName + ": " + (dirSize / 1e6).toInt + " MB\n")
		}
	
		return (IP.toString() + ":t" + n, s.toString(), 1)
	}
	else
		return (IP.toString() + ":t" + n, "Directory " + config.getTmpFolder() + " doesn't exist", 0)
}

def displayFileStats(n: Integer, config: Configuration) : (String, String, Integer) =
{
	val IP = InetAddress.getLocalHost()
	val d = new File(config.getTmpFolder())
	
	if (d.exists && d.isDirectory)
		return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getTmpFolder(), false), 1)
	else
		return (IP.toString() + ":t" + n, "Directory " + config.getTmpFolder() + " doesn't exist", 0)
}

def getFileNameFromPath(path: String) : String =
{
	return path.substring(path.lastIndexOf('/') + 1)
}

def getDirFromPath(path: String) : String =
{
	return path.substring(0, path.lastIndexOf('/') + 1)
}

def dbgLog(s: String, config: Configuration) =
{
	HDFSManager.append(config.getHadoopInstall, "sparkLog.txt", s + "\n")
}
	 
def main(args: Array[String]) 
{
	val config = new Configuration()
	config.initialize(args(0))

	val op = args(1)
	val nofInstances = config.getNumInstances().toInt
		 
	val conf = new SparkConf().setAppName("FilesDownloader")
	if (config.getMode() == "local")
	{
		println("No action required for local mode")
		System.exit(1)
	}
	val sc = new SparkContext(conf)
	val bcConfig = sc.broadcast(config)
	
	HDFSManager.create(config.getHadoopInstall, "sparkLog.txt")
	
	val r1 = 0 until nofInstances
	val keys = r1.toArray
	var optExecuted = "ls"
	val inputData = sc.parallelize(keys, nofInstances) 
	var result: org.apache.spark.rdd.RDD[(String, String, Integer)] = null
	
	if (op == "cpAll")
	{
		result = inputData.map(x=> downloadFiles(x, config))
		optExecuted = "cpAll"
	}
	else if (op == "rmAll")
	{
		result = inputData.map(x=> deleteFiles(x, config))
		optExecuted = "rmAll"
	}
	else if (op == "rmDir")
	{
		result = inputData.map(x=> deleteDir(x, config))
		optExecuted = "rmDir"
	}
	else if (op == "rmSubFolders")
	{
		result = inputData.map(x=> deleteSubFolders(x, config))
		optExecuted = "rmSubFolders"
	}
	else if (op == "rmJunk")
	{
		result = inputData.map(x=> deleteJunkFiles(x, config))
		optExecuted = "rmJunk"
	}
	else if (op == "rmExt")
	{
		result = inputData.map(x=> deleteExtension(x, args(2), config))
		optExecuted = "rmExt"
	}
	else if (op == "cp")
	{
		result = inputData.map(x=> copyFile(x, args(2), config))
		optExecuted = "cp"
	}
	else if (op == "lsDirs")
	{
		result = inputData.map(x=> listDirs(x, config))
		optExecuted = "lsDirs"
	}
	else
	{
		result = inputData.map(x=> displayFileStats(x, config))
		optExecuted = "ls"
	}
		
	val x = result.collect()
	val i = 0
	for( i <- 0  until x.size)
	{
		dbgLog("Node: " + x(i)._1, config)
		dbgLog(x(i)._2, config)
		dbgLog("r = " + x(i)._3, config)
	}
	
	dbgLog("Nodes touched:", config)
	dbgLog("Option: " + optExecuted, config)
	for( i <- 0  until x.size)
		dbgLog("\t" + x(i)._1, config)
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
