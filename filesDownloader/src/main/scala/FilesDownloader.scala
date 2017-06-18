/*
 * Copyright (C) 2016-2017 TU Delft, The Netherlands
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
 *
 * Authors: Hamid Mushtaq
 *
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

/**
 *
 * @author Hamid Mushtaq
 */
object FilesDownloader 
{
	//////////////////////////////////////////////////////////////////////////////
	def downloadFiles (n: Integer, config: Configuration) : (String, String, Integer) = 
	{
		val IP = InetAddress.getLocalHost
		var r = 0
		
		new File(config.getSfFolder).mkdirs
		
		val refFolder = config.getInputFolder
		val refFileName = config.getRefFileName
		r = r + HDFSManager.download(refFileName, refFolder, config.getSfFolder)
		r = r + HDFSManager.download(refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder)
		r = r + HDFSManager.download(refFileName + ".amb", refFolder, config.getSfFolder)
		r = r + HDFSManager.download(refFileName + ".ann", refFolder, config.getSfFolder)
		r = r + HDFSManager.download(refFileName + ".bwt", refFolder, config.getSfFolder)
		r = r + HDFSManager.download(refFileName + ".fai", refFolder, config.getSfFolder)
		r = r + HDFSManager.download(refFileName + ".pac", refFolder, config.getSfFolder)
		r = r + HDFSManager.download(refFileName + ".sa", refFolder, config.getSfFolder)
		
		val snpFileName = config.getSnpFileName
		r = r + HDFSManager.download(snpFileName, refFolder, config.getSfFolder)
		r = r + HDFSManager.download(snpFileName + ".idx", refFolder, config.getSfFolder)
		
		val indelFileName = config.getIndelFileName
		if (indelFileName != "")
		{
			r = r + HDFSManager.download(indelFileName, refFolder, config.getSfFolder)
			r = r + HDFSManager.download(indelFileName + ".idx", refFolder, config.getSfFolder)
		}
		
		return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getSfFolder, false), r)
	}

	def copyFile (n: Integer, fname: String, config: Configuration) : (String, String, Integer) = 
	{
		val IP = InetAddress.getLocalHost
		var r = 0
		
		new File(config.getSfFolder).mkdirs
		
		r = r + HDFSManager.download(fname, config.getInputFolder, config.getSfFolder)
		
		return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getSfFolder, false), r)
	}
	
	def deleteFile (n: Integer, fname: String, config: Configuration) : (String, String, Integer) = 
	{
		val IP = InetAddress.getLocalHost
		var r = 0
		
		val filePath = config.getSfFolder + fname
		new File(filePath).delete
		val f = new File(filePath)
		if (!f.exists)
			r = r + 1
		
		return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getSfFolder, false), r)
	}

	def deleteFiles (n: Integer, config: Configuration) : (String, String, Integer) = 
	{
		val IP = InetAddress.getLocalHost
		val d = new File(config.getSfFolder)
		
		if (d.exists && d.isDirectory) 
		{
			val list: List[File] = d.listFiles.filter(_.isFile).toList
			var i = 0
			var r = 0
				
			for(i <- 0 until list.size)
			{
				val filePath = config.getSfFolder + list(i).getName
				new File(filePath).delete
				val f = new File(filePath)
				if (!f.exists)
					r = r + 1
			}
				
			return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getSfFolder, false), r)
		} 
		else
			return (IP.toString() + ":t" + n, "Directory " + config.getSfFolder + " doesn't exist", 0)
	}

	def deleteDir(n: Integer, config: Configuration) : (String, String, Integer) = 
	{
		val IP = InetAddress.getLocalHost()
		val d = new File(config.getSfFolder)
		
		if (config.getSfFolder == "/tmp/")
			return (IP.toString() + ":t" + n, "Will not delete the /tmp folder!", 0)
		
		if (d.exists && d.isDirectory) 
		{
			FileUtils.deleteDirectory(d)
				
			return (IP.toString() + ":t" + n, "Directory " + config.getSfFolder + " deleted!", 1)
		} 
		else
			return (IP.toString() + ":t" + n, "Directory " + config.getSfFolder + " doesn't exist", 0)
	}

	def isJunkFile (fileName: String, config: Configuration) : Boolean = 
	{
		if (fileName.contains(config.getRefFileName.split('.')(0)))
			return false
		
		if (fileName.contains(config.getSnpFileName))
			return false
		
		if ((config.getIndelFileName != "") && fileName.contains(config.getIndelFileName))
			return false
		
		return true
	}

	def deleteJunkFiles (n: Integer, config: Configuration) : (String, String, Integer) = 
	{
		val IP = InetAddress.getLocalHost()
		val d = new File(config.getSfFolder())
		
		if (d.exists && d.isDirectory) 
		{
			val list: List[File] = d.listFiles.filter(_.isFile).toList
			var i = 0
			var r = 0
				
			for(i <- 0 until list.size)
			{
				if (isJunkFile(list(i).getName, config))
				{
					val filePath = config.getSfFolder + list(i).getName
					new File(filePath).delete()
					val f = new File(filePath)
					if (!f.exists)
						r = r + 1
				}
			}
				
			return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getSfFolder, false), r)
		} 
		else
			return (IP.toString() + ":t" + n, "Directory " + config.getSfFolder + " doesn't exist", 0)
	}

	def deleteExtension (n: Integer, ext: String, config: Configuration) : (String, String, Integer) = 
	{
		val IP = InetAddress.getLocalHost()
		val d = new File(config.getSfFolder())
				
		if (d.exists && d.isDirectory) 
		{
			val list: List[File] = d.listFiles.filter(_.isFile).toList
			var i = 0
			var r = 0
				
			for(i <- 0 until list.size)
			{
				val filePath = config.getSfFolder() + list(i).getName()
				val index = filePath.indexOf("." + ext)
				if (index != -1)
				{
					new File(filePath).delete()
					val f = new File(filePath)
					if (!f.exists())
						r = r + 1
				}
			}
				
			return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getSfFolder, false), r)
		} 
		else
			return (IP.toString() + ":t" + n, "Directory " + config.getSfFolder + " doesn't exist", 0)
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
		val d = new File(config.getSfFolder())
		
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
			return (IP.toString() + ":t" + n, "Directory " + config.getSfFolder() + " doesn't exist", 0)
	}

	def displayFileStats(n: Integer, config: Configuration) : (String, String, Integer) =
	{
		val IP = InetAddress.getLocalHost()
		val d = new File(config.getSfFolder())
		
		if (d.exists && d.isDirectory)
			return (IP.toString() + ":t" + n, HDFSManager.getLS(config.getSfFolder(), false), 1)
		else
			return (IP.toString() + ":t" + n, "Directory " + config.getSfFolder() + " doesn't exist", 0)
	}

	def dbgLog(s: String, config: Configuration) =
	{
		HDFSManager.append("sparkLog.txt", s + "\n")
	}
	 
	def main(args: Array[String]) 
	{
		val conf = new SparkConf().setAppName("FilesDownloader")
		val sc = new SparkContext(conf)
		val config = new Configuration()
		config.initialize(args(0), sc.deployMode)

		val op = args(1)
		val nofInstances = config.getNumNodes.toInt
			 
		if ((config.getMode != "yarn-client") && (config.getMode != "yarn-cluster"))
		{
			println("Wrong mode. It should be either yarn-client or yarn-cluster.")
			System.exit(1)
		}
		val bcConfig = sc.broadcast(config)
		
		HDFSManager.create("sparkLog.txt")
		
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
		else if (op == "rm")
		{
			result = inputData.map(x=> deleteFile(x, args(2), config))
			optExecuted = "rm"
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
