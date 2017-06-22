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
package hmushtaq.sparkga1

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import sys.process._
import org.apache.spark.scheduler._

import java.io._
import java.nio.file.{Paths, Files}
import java.net._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.sys.process.Process
import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable._
import scala.util.Sorting._
import scala.concurrent.Future
import scala.concurrent.forkjoin._
import scala.util.Random

import utils._

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.HashPartitioner

import htsjdk.samtools.util.BufferedLineReader
import htsjdk.samtools._

/**
 *
 * @author Hamid Mushtaq
 */
object SparkGA1
{
	final val saveAllStages = false
	final val downloadSAMFileInLB = true
	// Optional stages
	final val doIndelRealignment = true
	final val doPrintReads = true
	// Scheduling
	final val sizeBasedLBScheduling = true
	final val sizeBasedVCScheduling = true
	//
	final val downloadRef = false // Keep this false until the downloading part is implemented properly.
	//////////////////////////////////////////////////////////////////////////////
	def bwaRun(x: String, config: Configuration) : (Array[((Integer, Integer), (String, Long, Int, Int, String))]) = 
	{
		val blockSize = 4096 * 1024; 
		var input_file = ""
		val tmpDir = config.getTmpFolder
		val hdfsManager = new HDFSManager
		
		var t0 = System.currentTimeMillis
		
		if (config.getMode != "local")
		{
			hdfsManager.create(config.getOutputFolder + "log/bwa/" + x)
			
			if (!(new File(config.getTmpFolder).exists))
				new File(config.getTmpFolder).mkdirs()
			
			dbgLog("bwa/" + x, t0, "0a\tDownloading from the HDFS", config)
			hdfsManager.download(x + ".gz", config.getInputFolder, tmpDir, false)
			input_file = tmpDir + x + ".gz"
			if (downloadRef)
			{
				dbgLog("bwa/" + x, t0, "*\tDownloading reference files for bwa if required.", config)
				downloadBWAFiles("bwa/" + x, config)
			}
		}
		else
		{
			input_file = config.getInputFolder + x + ".gz"
			val file = new File(config.getOutputFolder + "log/bwa")
			if (!file.exists)
				file.mkdir()
		}
		
		val file = new File(getToolsDirPath(config) + "bwa") 
		file.setExecutable(true)
			
		// unzip the input .gz file
		var fqFileName = tmpDir + x
		val unzipStr = "gunzip -c " + input_file
		dbgLog("bwa/" + x, t0, "0b\t" + unzipStr, config)
		unzipStr #> new java.io.File(fqFileName) !;
		if (config.getMode != "local")
			new File(input_file).delete()
		
		// run bwa mem
		val progName = getToolsDirPath(config) + "bwa mem "
		val outFileName = tmpDir + "out_" + x
		val nthreads = config.getNumThreads.toInt
		// Example: bwa mem input_files_directory/fasta_file.fasta -p -t 2 x.fq > out_file
		val command_str = progName + getRefFilePath(config) + " " + config.getExtraBWAParams + " -t " + nthreads.toString + " " + fqFileName
		dbgLog("bwa/" + x, t0, "1\tbwa mem started, RGID = " + config.getRGID + " -> " + command_str, config)
		var writerMap = new HashMap[(Integer, Integer), SamRegion]()
		val samRegionsParser = new SamRegionsParser(x, writerMap, config)
		val logger = ProcessLogger(
			(o: String) => {
				samRegionsParser.append(o)
				},
			(e: String) => {} // do nothing
		)
		command_str ! logger;
		new File(fqFileName).delete()
		
		val res = ArrayBuffer.empty[((Integer, Integer), (String, Long, Int, Int, String))]
		makeDirIfRequired(config.getOutputFolder + "samChunks", config)
		makeDirIfRequired(config.getOutputFolder + "bwaPos", config)
		
		dbgLog("bwa/" + x, t0, "2\tUploading SAM Files to the HDFS. reads = " + samRegionsParser.getNumOfReads + ", bad lines = " + samRegionsParser.getBadLines, config)
		var currentNum = 1
		var outStr = new StringBuilder
		var dbgStr = new StringBuilder
		var totalBytes = 0
		// For positions
		var posCurrentNum = 1
		var posOutStr = new StringBuilder
		var posTotalBytes = 0
		val iWriterMap = writerMap.toMap // Might help with Garbage collection of the mutable HashMap.
		writerMap = null
		////////////////
		for((k,samRegion) <- iWriterMap)
		{
			val t1 = System.currentTimeMillis
			val chr = k._1
			val reg = k._2
			
			val minPos = samRegion.getMinPos
			val maxPos = samRegion.getMaxPos
			val content = samRegion.getContent
			val posContent = samRegion.getPositionsStr
			var posInfoStr = ""
			
			// For positions /////////////////////////////////////////////////////
			if ((posTotalBytes + posContent.size) > blockSize)
			{
				writeWholeFile(config.getOutputFolder + "bwaPos/pos_" + x + "-" + posCurrentNum, posOutStr.toString, config)
				posCurrentNum += 1
				posOutStr.setLength(0)
				posTotalBytes = 0
				posInfoStr = "pos_" + x + "-" + posCurrentNum + ",0," + posContent.size
			}
			else
			{
				posInfoStr = "pos_" + x + "-" + posCurrentNum + "," + posTotalBytes + "," + (posTotalBytes + posContent.size)
			}
			posOutStr.append(posContent)
			posTotalBytes += posContent.size
			//////////////////////////////////////////////////////////////////////
			if ((totalBytes + content.size) > 0)
			{
				//dbgStr.append("+writeWholeFile\t")
				writeWholeFile(config.getOutputFolder + "samChunks/chunk_" + x + "-" + currentNum, outStr.toString, config)
				currentNum += 1
				outStr.setLength(0)
				totalBytes = 0
				res.append(((chr, reg), ("chunk_" + x + "-" + currentNum + ",0," + content.size, samRegion.getSize, minPos, maxPos, posInfoStr)))
			}
			else
			{
				//dbgStr.append("-noWritingToWholeFile\t")
				res.append(((chr, reg), ("chunk_" + x + "-" + currentNum + "," + totalBytes + "," + (totalBytes + content.size), samRegion.getSize, 
					minPos, maxPos, posInfoStr)))
			}
			outStr.append(content)
			totalBytes += content.size
			//dbgStr.append((System.currentTimeMillis - t1).toString)
		}
		val t1 = System.currentTimeMillis
		writeWholeFile(config.getOutputFolder + "samChunks/chunk_" + x + "-" + currentNum, outStr.toString, config)
		writeWholeFile(config.getOutputFolder + "bwaPos/pos_" + x + "-" + posCurrentNum, posOutStr.toString, config)
		dbgStr.append("\n" + (System.currentTimeMillis - t1).toString)
		//dbgLog("bwa/" + x, t0, "*\tTime taken by each loop iteration for chunk making step = " + dbgStr, config)
		dbgLog("bwa/" + x, t0, "3\tSAM files uploaded to the HDFS. # of positions files = " + posCurrentNum + ", # of sam files = " + currentNum, config)

		return res.toArray
	}

	def getSamFileHeader(samFile: String) : String = 
	{
		val br = new BufferedReader(new FileReader(samFile))
		val sb = new StringBuilder
		var line = br.readLine()

		while ((line != null) && (line(0) == '@')) 
		{
			sb.append(line);
			sb.append(System.lineSeparator());
			line = br.readLine()
		}
		
		br.close()
		return sb.toString()
	}
		
	def makeBAMFiles(chrRegion: (Integer, Integer), files: Array[(String, Long, Int, Int, String)], avgReadsPerRegion: Long, config: Configuration) : 
		((Integer, Integer), Array[(String, Long)]) = 
	{
		val chr = chrRegion._1
		val reg = chrRegion._2
		val reads = files.map(x => x._2).reduce(_+_)
		var segments = (reads.toFloat * config.getRegionsFactor.toFloat / avgReadsPerRegion).round.toInt
		val retArray = ArrayBuffer.empty[(String, Long)]
		val minPos = files.map(x => x._3).reduceLeft(_ min _)
		val maxPos = files.map(x => x._4).reduceLeft(_ max _)
		val posRange = maxPos - minPos
		// Number of threads
		val nThreads = config.getNumThreads.toInt
		val hdfsManager = new HDFSManager
		
		if (config.getMode() != "local")
		{
			if (segments > 1)
				hdfsManager.create(config.getOutputFolder + "log/lb2/" + chr + "_" + reg)
			else
				hdfsManager.create(config.getOutputFolder + "log/lb/" + chr + "_" + reg)
		}
		else
		{
			makeDirIfRequired(config.getOutputFolder + "log/lb", config)
			makeDirIfRequired(config.getOutputFolder + "log/lb2", config)
		}
		
		if ((config.getMode != "local") && !(new File(config.getTmpFolder).exists))
			new File(config.getTmpFolder).mkdirs()
		
		var t0 = System.currentTimeMillis
		
		if (segments > 1)
		{
			dbgLog("lb2/" + chr + "_" + reg, t0, "1a\tNumber of reads = " + reads + ", avgReadsPerRegion = " + avgReadsPerRegion + 
				", number of segments > 1 (" + segments + ")!", config);
			dbgLog("lb2/" + chr + "_" + reg, t0, "1b\tminPos = " + minPos + ", maxPos = " + maxPos + ", posRange = " + posRange, config);
		}
		else
			dbgLog("lb/" + chr + "_" + reg, t0, "1\tStarting to combine the files. Reads = " + reads + ", avgReadsPerRegion = " + 
				avgReadsPerRegion, config);
		
		val samRecords = ArrayBuffer.empty[(Integer, SAMRecord)]
		var fileCount = 0
		val shuffledFiles = Random.shuffle(files.toList).toArray
		// Sam writers for each segment of a region with more than one segments
		val samWriters = ArrayBuffer.empty[SAMFileWriter]
		// Alignment positions based on which Multisegmented region would be divided.
		var alnPosArray: Array[Int] = null
		// Reads writen per segment
		val readsWritten = ArrayBuffer.empty[Long]
		
		// Initialize the data structures for a region with more than one segments
		if (segments > 1)
		{
			val header = createHeader(config)
			
			for(a <- 0 until segments)
			{
				val fileName = config.getTmpFolder + chr + "_" + reg + "_part" + a + ".sam"
				val factory = new SAMFileWriterFactory()
				val writer = factory.makeSAMWriter(header, false, new File(fileName))
				// Append a writer for creating a segment of a region
				samWriters.append(writer)
				// Initialize reads count for each segment
				readsWritten.append(0)
			}
			
			dbgLog("lb2/" + chr + "_" + reg, t0, "2a\tGetting the positions. Number of input files = " + shuffledFiles.size, config)
			val contentArray = new Array[StringBuilder](nThreads) 
			val fileInfoPerThread = ArrayBuffer.empty[scala.collection.mutable.ArrayBuffer[String]]
			
			for( a <- 0 until nThreads)
			{
				fileInfoPerThread.append(ArrayBuffer.empty[String])
				contentArray(a) = new StringBuilder
			}
			
			var index = 0
			for (file <- shuffledFiles)
			{
				fileInfoPerThread(index % nThreads).append(file._5)
				index += 1
			}
			
			val threadArray = new Array[Thread](nThreads)
			for(thread <- 0 until nThreads)
			{
				// Multithreaded /////////////////////////////////////////////////
				threadArray(thread) = new Thread {
					override def run {
				//////////////////////////////////////////////////////////////////
				for(f <- fileInfoPerThread(thread))
				{
					val fileInfo = f.split(',')
					contentArray(thread).append(readPartialFile(config.getOutputFolder + "bwaPos/" + fileInfo(0), fileInfo(2).toInt, config).slice(fileInfo(1).toInt, fileInfo(2).toInt))
				}
				// Multithreaded /////////////////////////////////////////////////
					}
				}
				threadArray(thread).start
				//////////////////////////////////////////////////////////////////
			}
			// Multithreaded ///////////////////////////////////////////////// 
			for(thread <- 0 until nThreads)
				threadArray(thread).join
			//////////////////////////////////////////////////////////////////
			
			val content = new StringBuilder
			for(thread <- 0 until nThreads)
				content.append(contentArray(thread))
			alnPosArray = content.split('\n').map(x => x.toInt)
			dbgLog("lb2/" + chr + "_" + reg, t0, "2b\tGot all " + alnPosArray.size + " positions in an array.", config)
		}
				
		for (file <- shuffledFiles)
		{
			val fileInfo = file._1.split(',')
			
			if (segments > 1)	// For a region with more than one segment
				retArray.append((file._1, segments))
			else 
			{	
				//val content = readWholeFile(config.getOutputFolder + "samChunks/" + fileInfo(0), config).slice(fileInfo(1).toInt, fileInfo(2).toInt)
				val content = readPartialFile(config.getOutputFolder + "samChunks/" + fileInfo(0), fileInfo(2).toInt, config).slice(fileInfo(1).toInt, fileInfo(2).toInt)
				
				// Get sam records from the chunk
				val bwaKeyValues = new SamRecsReader(new StringBufferInputStream(content), config)	
				bwaKeyValues.parseSam(null)
				val kvPairs: Array[(Integer, SAMRecord)] = bwaKeyValues.getKeyValuePairs()
				bwaKeyValues.close()
			
				for (e <- kvPairs)
					samRecords.append(e)
				
				fileCount += 1
				if ((fileCount % 50) == 0)
					dbgLog("lb/" + chr + "_" + reg, t0, "2\tRead " + fileCount + " files into the array.", config)
			}
		} // End of for
		
		if (segments <= 1) // For a region with just one segment
		{
			dbgLog("lb/" + chr + "_" + reg, t0, "3\tRead all " + fileCount + " files into the array.", config)
			writeToBAMAndBed(chr + "_" + reg, samRecords.toArray, 0, samRecords.size, config)
			dbgLog("lb/" + chr + "_" + reg, t0, "4\t" + fileCount + " SAM files processed. Combined file uploaded to the HDFS.", config)
		}
		else 
		{
			val readsPerSegment = alnPosArray.size / segments
			
			// Sort the position array, so that we can use binary search with it
			scala.util.Sorting.stableSort(alnPosArray)
			
			dbgLog("lb2/" + chr + "_" + reg, t0, "3a\tTotal number of reads = " + alnPosArray.size + ", reads per segment = " + readsPerSegment, config)
			
			val fileInfoPerThread = ArrayBuffer.empty[scala.collection.mutable.ArrayBuffer[String]]
			
			for( a <- 0 until nThreads)
				fileInfoPerThread.append(ArrayBuffer.empty[String])
			
			var index = 0
			for (file <- shuffledFiles)
			{
				fileInfoPerThread(index % nThreads).append(file._1)
				index += 1
			}
			
			val threadArray = new Array[Thread](nThreads)
			for(thread <- 0 until nThreads)
			{
				// Multithreaded /////////////////////////////////////////////////
				threadArray(thread) = new Thread {
					override def run {
				//////////////////////////////////////////////////////////////////
				var fileCount = 0
				for(f <- fileInfoPerThread(thread))
				{
					val fileInfo = f.split(',')
					var kvPairs: Array[(Integer, SAMRecord)] = null
					
					if (!downloadSAMFileInLB)
					{
						val content = readWholeFile(config.getOutputFolder + "samChunks/" + fileInfo(0), config).slice(fileInfo(1).toInt, fileInfo(2).toInt)
				
						// Get sam records from the chunk
						val bwaKeyValues = new SamRecsReader(new StringBufferInputStream(content), config)			
						bwaKeyValues.parseSam(null)
						kvPairs = bwaKeyValues.getKeyValuePairs()
						bwaKeyValues.close()
					}
					else
					{
						val fname = config.getTmpFolder + fileInfo(0)
						hdfsManager.download(fileInfo(0), config.getOutputFolder + "samChunks/", config.getTmpFolder, false)
						
						// Get sam records from the chunk
						val bwaKeyValues = new SamRecsReader(new FileInputStream(fname), config)			
						bwaKeyValues.parseSam(null)
						kvPairs = bwaKeyValues.getKeyValuePairs()
						bwaKeyValues.close()
						
						new File(fname).delete
					}
					
					for (e <- kvPairs)
					{
						var i = binarySearch(alnPosArray, 0, alnPosArray.size-1, e._2.getAlignmentStart) / readsPerSegment
						
						if (i >= segments)
							i = segments-1
						
						samWriters(i).synchronized
						{
							// Write to corresponding segment
							samWriters(i).addAlignment(e._2)
							readsWritten(i) += 1
						}
					}
						
					fileCount += 1
					if ((fileCount % 50) == 0)
						files.synchronized {dbgLog("lb2/" + chr + "_" + reg, t0, "3b\t" + fileCount + " files and records written by thread " + thread, config)}
				}
				// Multithreaded /////////////////////////////////////////////////
					}
				}
				threadArray(thread).start
				//////////////////////////////////////////////////////////////////
			}
			// Multithreaded ///////////////////////////////////////////////// 
			for(thread <- 0 until nThreads)
				threadArray(thread).join
			//////////////////////////////////////////////////////////////////
			
			// Make bam files of all the segments
			val iterations =  Math.ceil(segments/nThreads.toFloat).toInt 
			for(iter <- 0 until iterations)
			{
				val threadArray = new Array[Thread](nThreads)
				var nUsedThreads = 0
				for(thread <- 0 until nThreads)
				{
					val a = iter * nThreads + thread
					if (a < segments)
					{
						nUsedThreads += 1
						// Multithreaded /////////////////////////////////////////////////
						threadArray(thread) = new Thread {
							override def run {
						//////////////////////////////////////////////////////////////////
						val inputFile = config.getTmpFolder + chr + "_" + reg + "_part" + a + ".sam"
						samWriters(a).close()
					
						files.synchronized {dbgLog("lb2/" + chr + "_" + reg, t0, "4a\tReads in segment " + a + " = " + readsWritten(a), config)}
						// Get sam records from the segment
						val bwaKeyValues = new SamRecsReader(new FileInputStream(inputFile), config)
						bwaKeyValues.parseSam(null)
						val samRecords: Array[(Integer, SAMRecord)] = bwaKeyValues.getKeyValuePairs()
						bwaKeyValues.close()
			
						files.synchronized {dbgLog("lb2/" + chr + "_" + reg, t0, "4b\tWriting to BAM segment" + a + ". nUsedThreads = " + nUsedThreads, config)}
						writeToBAMAndBed("part" + a + "_" + chr + "_" + reg, samRecords.toArray, 0, samRecords.size, config)
						files.synchronized {dbgLog("lb2/" + chr + "_" + reg, t0, "4c\tDone writing " + samRecords.size + " reads from " + inputFile, config)}
					
						new File(inputFile).delete()
						// Multithreaded /////////////////////////////////////////////////
							}
						}
						threadArray(thread).start
						//////////////////////////////////////////////////////////////////
					}
				}
				
				// Multithreaded /////////////////////////////////////////////////
				for(thread <- 0 until nUsedThreads)
					threadArray(thread).join
				//////////////////////////////////////////////////////////////////
			}
			
			dbgLog("lb2/" + chr + "_" + reg, t0, "5\tDone writing contents of all sam files.", config)
		}
		
		return ((chr, reg), retArray.toArray)
	}

	def binarySearch(arr: Array[Int], starti: Int, endi: Int, x: Int) : Int =
	{
		if (starti > endi)
			return 0 // If element is not found, we will just put it in segment #0
		
		val guess = (starti + endi) / 2
		
		if (arr(guess) == x)
			return guess
		
		if ((guess != 0) && (arr(guess-1) == x))
			return guess - 1
			
		if ((guess != endi) && (arr(guess+1) == x))
			return guess + 1
			
		if (arr(guess) > x)
			return binarySearch(arr, starti, guess-1, x)
		else
			return binarySearch(arr, guess+1, endi, x) 
	}

	def writeToBAMAndBed(chrRegion: String, samRecords: Array[(Integer, SAMRecord)], startIndex: Int, endIndex: Int, config: Configuration) : 
		Integer =
	{
		val tmpFileBase =  config.getTmpFolder + chrRegion
		val header = createHeader(config)
		header.setSortOrder(SAMFileHeader.SortOrder.coordinate)
		//////////////////////////////
		val bamrg = new SAMReadGroupRecord(config.getRGID)
		bamrg.setLibrary("LIB1")
		bamrg.setPlatform("ILLUMINA")
		bamrg.setPlatformUnit("UNIT1")
		bamrg.setSample("SAMPLE1")
		header.addReadGroup(bamrg)
		/////////////////////////////
		var part = if (chrRegion.contains("part")) "2" else "";
		val hdfsManager = new HDFSManager
			
		if (config.getMode != "local")
			hdfsManager.create(config.getOutputFolder + "log/lb" + part + "/region_" + chrRegion)
		else
			makeDirIfRequired(config.getOutputFolder + "log/lb" + part, config)

		var t0 = System.currentTimeMillis
		
		if (config.isInIgnoreList(samRecords(0)._2.getReferenceName))
		{
			dbgLog("lb" + part + "/region_" + chrRegion, t0, "0\tChromosome " + samRecords(0)._2.getReferenceName + 
				" is being ignored!", config);
			return 0
		}
		
		// Sorting
		implicit val samRecordOrdering = new Ordering[(Integer, SAMRecord)] {
			override def compare(a: (Integer, SAMRecord), b: (Integer, SAMRecord)) = compareSAMRecords(a._2, b._2)
		}
		if ((startIndex) == 0 && (endIndex == samRecords.size))
		{
			dbgLog("lb" + part + "/region_" + chrRegion, t0, "0a\t" + "quicksort started...", config)
			scala.util.Sorting.stableSort(samRecords)
			dbgLog("lb" + part + "/region_" + chrRegion, t0, "0b\t" + "quicksort done!", config)
		}
		//
		
		val factory = new SAMFileWriterFactory()
		val writer = factory.makeBAMWriter(header, true, new File(tmpFileBase + "-p1.bam"))
		val regionIter = new RegionIterator(samRecords, header, startIndex, endIndex)
		val RGID = config.getRGID
		var count = 0
		var badLines = 0
		while(regionIter.hasNext()) 
		{
			val sam = regionIter.next()
			/////////////////////////////////////////
			sam.setAttribute(SAMTag.RG.name(), RGID)
			/////////////////////////////////////////
			try
			{
				writer.addAlignment(sam)
			}
			catch
			{
				case e: Exception => badLines += 1
			}
			count += 1
			if ((count % 500000) == 0)
				dbgLog("lb" + part + "/region_" + chrRegion, t0, "1\t" + count + " records written. Bad lines = " + badLines, config)
		}
		regionIter.addLastChrRange()
		val reads = regionIter.getCount()
		writer.close()
		
		dbgLog("lb" + part + "/region_" + chrRegion, t0, "2\tMaking bed file. Reads = " + reads + ", written = " + count, config)
		makeRegionFile(tmpFileBase, regionIter, config)
		
		uploadFileToOutput(tmpFileBase + "-p1.bam", "bam", true, config)
		uploadFileToOutput(tmpFileBase + ".bed", "bed", true, config)
		dbgLog("lb" + part + "/region_" + chrRegion, t0, "3\tBAM and bed files uploaded to the HDFS", config)
		
		return reads
	}

	def copyExomeBed(exomeBed: String, config: Configuration)
	{
		val lines = readWholeFile(config.getExomePath, config)
		var from = 0
			
		if (lines(0) == '@')
		{
			var done = false
			
			while(!done)
			{
				var next = lines.indexOf('\n', from)
				from = next + 1
				if (lines(from) != '@')
					done = true
			}
		}
			
		new PrintWriter(exomeBed) {write(lines.substring(from)); close}
	}

	def makeCorrectBedFile(cmdStr: String, bedFile: String)
	{
		val cmdRes = (cmdStr #> new java.io.File(bedFile + ".1")).!
		val lines = scala.io.Source.fromFile(bedFile + ".1").mkString.split('\n')
		var s = ""
		
		for( line <- lines)
		{
			val e = line.split('\t')
			if (e.size >= 2)
			{
				val lval = e(1).toLong
				var hval = e(2).toLong
			
				if (lval == hval)
					hval += 1
			
				s += e(0) + "\t" + lval + "\t" + hval + "\n"
			}
		}
		
		new PrintWriter(bedFile) {write(s); close}
		new File(bedFile + ".1").delete()
	}

	def makeRegionFile(tmpFileBase: String, regionIter: RegionIterator, config: Configuration)
	{
		val bedFile = tmpFileBase + ".bed"
		val hdfsManager = new HDFSManager
		
		if (config.useExome())
		{
			val toolsFolder = getToolsDirPath(config)
			val exomeBed = tmpFileBase + "_exome.bed"
			
			copyExomeBed(exomeBed, config)
			
			val file = new File(getToolsDirPath(config) + "bedtools") 
			file.setExecutable(true)
			
			 // write a bed file with the region!
			val bed = new File(exomeBed + "_tmp.bed")
			regionIter.writeToBedFile(bed.getAbsolutePath())
			makeCorrectBedFile(toolsFolder + "bedtools intersect -a " + exomeBed + " -b " + bed + " -header", bedFile)
			
			// Delete temporary files
			new File(exomeBed).delete()
			new File(exomeBed + "_tmp.bed").delete()
		}
		else
		{
			val bed = new File(bedFile)
			regionIter.writeToBedFile(bed.getAbsolutePath())
		}
	}

	def createSAMWriter(fileName: String, config: Configuration) : SAMFileWriter =
	{
		val header = createHeader(config)
		val factory = new SAMFileWriterFactory()
		return factory.makeSAMWriter(header, false, new File(fileName))
	}

	def compareSAMRecords(a: SAMRecord, b: SAMRecord) : Int = 
	{
		if(a.getReferenceIndex == b.getReferenceIndex)
			return a.getAlignmentStart - b.getAlignmentStart
		else
			return a.getReferenceIndex - b.getReferenceIndex
	}

	def createHeader(config: Configuration) : SAMFileHeader =
	{
		val header = new SAMFileHeader()
		header.setSequenceDictionary(config.getDict())
		
		return header
	}

	def uploadFileToOutput(filePath: String, outputPath: String, delSrc: Boolean, config: Configuration)
	{
		if (config.getMode() != "local")
		{
			val fileName = getFileNameFromPath(filePath)
			new File(config.getTmpFolder + "." + fileName + ".crc").delete()
			val hdfsManager = new HDFSManager
			
			hdfsManager.upload(fileName, config.getTmpFolder, config.getOutputFolder + outputPath + "/", delSrc) 
		}
	}

	def makeDirIfRequired(dir: String, config: Configuration)
	{
		if (config.getMode == "local")
		{
			val file = new File(dir)
			if (!file.exists())
				file.mkdir()
		}			
	}

	def processBAM(chrRegion: String, config: Configuration) : Integer =
	{
		val tmpFileBase = config.getTmpFolder + chrRegion
		var t0 = System.currentTimeMillis
		val hdfsManager = new HDFSManager
		
		if (config.getMode != "local")
		{
			hdfsManager.create(config.getOutputFolder + "log/" + "region_" + chrRegion)
				
			if (!(new File(config.getTmpFolder).exists))
				new File(config.getTmpFolder).mkdirs()
			
			dbgLog("region_" + chrRegion, t0, "2g\tDownloading bam and bed files to the local directory...", config)
			hdfsManager.download(chrRegion + "-p1.bam", config.getOutputFolder + "bam/", config.getTmpFolder, false)
			hdfsManager.download(chrRegion + ".bed", config.getOutputFolder + "bed/", config.getTmpFolder, false)
			dbgLog("region_" + chrRegion, t0, "2h\tCompleted download of bam and bed to the local directory!", config)
		}
		
		var f = new File(tmpFileBase + "-p1.bam");
		if(f.exists() && !f.isDirectory()) 
			dbgLog("region_" + chrRegion, t0, "*+\tBAM file does exist in the tmp directory!", config)
		else
			dbgLog("region_" + chrRegion, t0, "*-\tBAM file does not exist in the tmp directory!", config)
		
		f = new File(tmpFileBase + ".bed");
		if(f.exists() && !f.isDirectory()) 
			dbgLog("region_" + chrRegion, t0, "#+\tbed file does exist in the tmp directory!!", config)
		else
			dbgLog("region_" + chrRegion, t0, "#-\tbed file does not exist in the tmp directory!!", config)
		
		dbgLog("region_" + chrRegion, t0, "3\tPicard processing started", config)
		var cmdRes = picardPreprocess(tmpFileBase, config)
		if (downloadRef)
		{
			dbgLog("region_" + chrRegion, t0, "*\tDownloading VCF ref files", config)
			downloadVCFRefFiles("region_" + chrRegion, config)
		}
		if (doIndelRealignment)
			cmdRes += indelRealignment(tmpFileBase, t0, chrRegion, config)
		if (downloadRef)
		{
			dbgLog("region_" + chrRegion, t0, "*\tDownloading snp file", config)
			downloadVCFSnpFile("region_" + chrRegion, config)
		}
		cmdRes += baseQualityScoreRecalibration(tmpFileBase, t0, chrRegion, config)
		cmdRes += DnaVariantCalling(tmpFileBase, t0, chrRegion, config)
		
		if (config.getMode() != "local")
		{
			new File(config.getTmpFolder() + "." + chrRegion + ".vcf.crc").delete()
			hdfsManager.upload(chrRegion + ".vcf", config.getTmpFolder, config.getOutputFolder, true)
		}
		
		dbgLog("region_" + chrRegion, t0, "9\tOutput written to vcf file", config)
		return cmdRes
	}

	def variantCall (chrRegion: String, config: Configuration) : (String, Integer) =
	{
		return (chrRegion, processBAM(chrRegion, config))
	}

	def picardPreprocess(tmpFileBase: String, config: Configuration) : Integer =
	{
		val toolsFolder = getToolsDirPath(config)
		val tmpOut1 = tmpFileBase + "-p1.bam"
		val tmpOut2 = tmpFileBase + "-p2.bam"
		val MemString = config.getExecMemX()
		
		var t0 = System.currentTimeMillis
		
		var cmdStr = "java " + MemString + " -jar " + toolsFolder + "CleanSam.jar INPUT=" + tmpOut1 + " OUTPUT=" + tmpOut2
		var cmdRes = cmdStr.!
		
		val bamOut = tmpFileBase + ".bam"
		val tmpMetrics = tmpFileBase + "-metrics.txt"
		
		cmdStr = "java " + MemString + " -jar " + toolsFolder + "MarkDuplicates.jar INPUT=" + tmpOut2 + " OUTPUT=" + bamOut +
			" METRICS_FILE=" + tmpMetrics + " CREATE_INDEX=true";
		cmdRes = cmdStr.!
		
		// Hamid - Save output of picardPreprocessing
		if (saveAllStages)
			uploadFileToOutput(bamOut, "picardOutput", false, config)
		
		// Delete temporary files
		new File(tmpOut1).delete()
		new File(tmpOut2).delete()
		new File(tmpMetrics).delete()
		
		return cmdRes
	}

	def indelRealignment(tmpFileBase: String, t0: Long, chrRegion: String, config: Configuration) : Integer =
	{
		val toolsFolder = getToolsDirPath(config)
		val knownIndel = getIndelFilePath(config)
		val tmpFile1 = tmpFileBase + "-2.bam"
		val preprocess = tmpFileBase + ".bam"
		val targets = tmpFileBase + ".intervals"
		val MemString = config.getExecMemX()
		val regionStr = " -L " + tmpFileBase + ".bed"
		val indelStr = if (config.useKnownIndels) (" -known " + knownIndel) else ""; 
		
		// Realigner target creator
		var cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T RealignerTargetCreator -nt " + 
		config.getNumThreads() + " -R " + getRefFilePath(config) + " -I " + preprocess + indelStr + " -o " +
			targets + regionStr
		dbgLog("region_" + chrRegion, t0, "4\t" + cmdStr, config)
		var cmdRes = cmdStr.!
		
		// Indel realigner
		cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T IndelRealigner -R " + 
			getRefFilePath(config) + " -I " + preprocess + " -targetIntervals " + targets + indelStr + " -o " + tmpFile1 + regionStr
		dbgLog("region_" + chrRegion, t0, "5\t" + cmdStr, config)
		cmdRes += cmdStr.!
		
		// Hamid - Save output of indelRealignment
		if (saveAllStages)
			uploadFileToOutput(tmpFile1, "indelOutput", false, config)
		
		// Delete temporary files
		new File(preprocess).delete()
		new File(preprocess.replace(".bam", ".bai")).delete()
		new File(targets).delete()
		
		return cmdRes
	}

	def baseQualityScoreRecalibration(tmpFileBase: String, t0: Long, chrRegion: String, config: Configuration) : Integer =
	{
		val toolsFolder = getToolsDirPath(config)
		val knownIndel = getIndelFilePath(config)
		val knownSite = getSnpFilePath(config)
		val tmpFile1 = if (doIndelRealignment) (tmpFileBase + "-2.bam") else (tmpFileBase + ".bam")
		val tmpFile2 = tmpFileBase + "-3.bam"
		val table = tmpFileBase + ".table"
		val MemString = config.getExecMemX()
		val regionStr = " -L " + tmpFileBase + ".bed"
		val indelStr = if (config.useKnownIndels) (" -knownSites " + knownIndel) else ""; 
		
		// Base recalibrator
		var cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T BaseRecalibrator -nct " + 
			config.getNumThreads() + " -R " + getRefFilePath(config) + " -I " + tmpFile1 + " -o " + table + regionStr + 
			" --disable_auto_index_creation_and_locking_when_reading_rods" + indelStr + " -knownSites " + knownSite
		dbgLog("region_" + chrRegion, t0, "6\t" + cmdStr, config)
		var cmdRes = cmdStr.!

		if (doPrintReads)
		{
			cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T PrintReads -R " + 
				getRefFilePath(config) + " -I " + tmpFile1 + " -o " + tmpFile2 + " -BQSR " + table + regionStr 
			dbgLog("region_" + chrRegion, t0, "7\t" + cmdStr, config)
			cmdRes += cmdStr.!
			// Hamid - Save output of baseQualityScoreRecalibration
			if (saveAllStages)
				uploadFileToOutput(tmpFile2, "baseOutput", false, config)
		
			// Delete temporary files
			new File(tmpFile1).delete()
			new File(tmpFile1.replace(".bam", ".bai")).delete()
			new File(table).delete()
		}
		
		return cmdRes
	}

	def DnaVariantCalling(tmpFileBase: String, t0: Long, chrRegion: String, config: Configuration) : Integer =
	{
		val toolsFolder = getToolsDirPath(config)
		val tmpFile2 = if (doPrintReads) (tmpFileBase + "-3.bam") else if (doIndelRealignment) (tmpFileBase + "-2.bam") else (tmpFileBase + ".bam")
		val snps = tmpFileBase + ".vcf"
		val bqsrStr = if (doPrintReads) "" else (" -BQSR " + tmpFileBase + ".table ")
		val MemString = config.getExecMemX()
		val regionStr = " -L " + tmpFileBase + ".bed"
		
		// Haplotype caller
		var cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T HaplotypeCaller -nct " + 
			config.getNumThreads() + " -R " + getRefFilePath(config) + " -I " + tmpFile2 + bqsrStr + " --genotyping_mode DISCOVERY -o " + snps + 
			" -stand_call_conf " + config.getSCC() + " -stand_emit_conf " + config.getSEC() + regionStr + 
			" --no_cmdline_in_header --disable_auto_index_creation_and_locking_when_reading_rods"
		dbgLog("region_" + chrRegion, t0, "8\t" + cmdStr, config)
		var cmdRes = cmdStr.!
		
		// Delete temporary files
		new File(tmpFile2).delete()
		new File(tmpFile2.replace(".bam", ".bai")).delete()
		new File(tmpFileBase + ".bed").delete()
		if (!doPrintReads)
			new File(tmpFileBase + ".table").delete
		
		return cmdRes
	}

	def getVCF(chrRegion: String, config: Configuration) : Array[((Integer, Integer), String)] =
	{
		var a = scala.collection.mutable.ArrayBuffer.empty[((Integer, Integer), String)]
		var fileName = config.getTmpFolder() + chrRegion + ".vcf"
		var commentPos = 0
		val hdfsManager = new HDFSManager
		
		if (config.getMode() != "local")
			hdfsManager.download(chrRegion + ".vcf", config.getOutputFolder, config.getTmpFolder, false)
		
		if (!Files.exists(Paths.get(fileName)))
			return a.toArray
		
		for (line <- Source.fromFile(fileName).getLines()) 
		{
			val c = line(0)
			if (c != '#')
			{
				val e = line.split('\t')
				val position = e(1).toInt
				var chromosome = e(0)
				var chrNumber = config.getChrIndex(chromosome)
					
				a.append(((chrNumber, position), line))
			}
			else
			{
				a.append(((-1, commentPos), line))
				commentPos = commentPos + 1
			}
		}
		
		// Delete temporary files
		if (config.getMode != "local")
			new File(fileName).delete()
		new File(fileName + ".idx").delete()
		
		return a.toArray
	}
		
	def getInputFileNames(dir: String, config: Configuration) : Array[String] = 
	{
		val mode = config.getMode
		val hdfsManager = new HDFSManager
		
		if (mode != "local")
		{
			val a: Array[String] = hdfsManager.getFileList(dir)

			return a
		}
		else
		{
			var d = new File(dir)	
			
			if (d.exists && d.isDirectory) 
			{
				val list: List[File] = d.listFiles.filter(_.isFile).toList
				val a: Array[String] = new Array[String](list.size)
				var i = 0
				
				for(i <- 0 until list.size)
					a(i) = list(i).getName
				
				return a
			} 
			else
				return null
		}
	}

	def getFileNameFromPath(path: String) : String =
	{
		return path.substring(path.lastIndexOf('/') + 1)
	}

	def getDirFromPath(path: String) : String =
	{
		return path.substring(0, path.lastIndexOf('/') + 1)
	}

	def getRefFilePath(config: Configuration) : String = 
	{
		return if (config.getMode() == "local") config.getRefPath() else 
			config.getSfFolder() + getFileNameFromPath(config.getRefPath())
	}

	def getSnpFilePath(config: Configuration) : String = 
	{
		return if (config.getMode() == "local") config.getSnpPath() else
			config.getSfFolder() + getFileNameFromPath(config.getSnpPath())
	}

	def getIndelFilePath(config: Configuration) : String = 
	{
		return if (config.getMode() == "local") config.getIndelPath() else
			config.getSfFolder() + getFileNameFromPath(config.getIndelPath())
	}

	def getExomeFilePath(config: Configuration) : String = 
	{
		return if (config.getMode() == "local") config.getExomePath() else
			config.getSfFolder() + getFileNameFromPath(config.getExomePath())
	}

	def getToolsDirPath(config: Configuration) : String = 
	{
		return if (config.getMode() == "local") config.getToolsFolder() else "./"
	}

	def getTimeStamp() : String =
	{
		return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())
	}

	def log(fname: String, key: String, t0: Long, message: String, config: Configuration) = 
	{
		val ct = System.currentTimeMillis
		val at = (ct - config.getStartTime()) / 1000
		val hdfsManager = new HDFSManager
		
		if (config.getMode != "local")
		{
			val IP = InetAddress.getLocalHost().toString()
			val node = IP.substring(0, IP.indexOf('/'))
			// Node, time, absolute time, key, message
			hdfsManager.append(fname, node + "\t" + getTimeStamp() + "\t" + 
				at.toString() + "\t" + message + "\n")
		}
		else
		{
			val s = getTimeStamp() + "\t" + at.toString() + "\t" + message
			println(s)
			val fw = new FileWriter(fname, true) 
			fw.write(s + "\n") 
			fw.close()
		}
	}

	def statusLog(key: String, t0: Long, message: String, config: Configuration) =
	{
		log("sparkLog.txt", key, t0, key + "\t" + message, config)
	}

	def dbgLog(key: String, t0: Long, message: String, config: Configuration) =
	{
		log(config.getOutputFolder + "log/" + key, key, t0, message, config)
	}

	def errLog(key: String, t0: Long, message: String, config: Configuration) =
	{
		log("errorLog.txt", key, t0, key + "\t" + message, config)
	}

	def readWholeFile(fname: String, config: Configuration) : String =
	{
		val hdfsManager = new HDFSManager
		
		if (config.getMode != "local")
			return hdfsManager.readWholeFile(fname)
		else
			return scala.io.Source.fromFile(fname).mkString
	}

	def readPartialFile(fname: String, bytes: Int, config: Configuration) : String =
	{
		val hdfsManager = new HDFSManager
		
		if (config.getMode != "local")
			return hdfsManager.readPartialFile(fname, bytes)
		else
			return scala.io.Source.fromFile(fname).mkString
	}

	def writeWholeFile(fname: String, s: String, config: Configuration)
	{
		val hdfsManager = new HDFSManager
		
		if (config.getMode != "local")
			hdfsManager.writeWholeFile(fname, s)
		else
			new PrintWriter(fname) {write(s); close}
	}

	def downloadBWAFiles(x: String, config: Configuration)
	{
		val refFolder = getDirFromPath(config.getRefPath())
		val refFileName = getFileNameFromPath(config.getRefPath())
		val hdfsManager = new HDFSManager
		
		if (!(new File(config.getSfFolder).exists))
			new File(config.getSfFolder()).mkdirs()
		
		hdfsManager.downloadIfRequired(refFileName, refFolder, config.getSfFolder);
		hdfsManager.downloadIfRequired(refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder)
		hdfsManager.downloadIfRequired(refFileName + ".amb", refFolder, config.getSfFolder)
		hdfsManager.downloadIfRequired(refFileName + ".ann", refFolder, config.getSfFolder)
		hdfsManager.downloadIfRequired(refFileName + ".bwt", refFolder, config.getSfFolder);
		hdfsManager.downloadIfRequired(refFileName + ".fai", refFolder, config.getSfFolder)
		hdfsManager.downloadIfRequired(refFileName + ".pac", refFolder, config.getSfFolder)
		hdfsManager.downloadIfRequired(refFileName + ".sa", refFolder, config.getSfFolder)
	}

	def downloadVCFRefFiles(x: String, config: Configuration)
	{
		val refFolder = getDirFromPath(config.getRefPath())
		val refFileName = getFileNameFromPath(config.getRefPath())
		val hdfsManager = new HDFSManager
		
		if (!(new File(config.getSfFolder).exists))
			new File(config.getSfFolder()).mkdirs()
		
		hdfsManager.downloadIfRequired(refFileName, refFolder, config.getSfFolder);
		hdfsManager.downloadIfRequired(refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder)
		hdfsManager.downloadIfRequired(refFileName + ".fai", refFolder, config.getSfFolder)
		
		if (config.useKnownIndels)
		{
			val indelFolder = getDirFromPath(config.getIndelPath())
			val indelFileName = getFileNameFromPath(config.getIndelPath())
			hdfsManager.downloadIfRequired(indelFileName, indelFolder, config.getSfFolder())
			hdfsManager.download(indelFileName + ".idx", indelFolder, config.getSfFolder, true)
		}
	}

	def downloadVCFSnpFile(x: String, config: Configuration)
	{
		val snpFolder = getDirFromPath(config.getSnpPath)
		val snpFileName = getFileNameFromPath(config.getSnpPath)
		val hdfsManager = new HDFSManager
		
		hdfsManager.downloadIfRequired(snpFileName, snpFolder, config.getSfFolder);
		hdfsManager.download(snpFileName + ".idx", snpFolder, config.getSfFolder, true)	
	}

	def main(args: Array[String]) 
	{
		val conf = new SparkConf().setAppName("SparkGA1")
		val sc = new SparkContext(conf)
		val config = new Configuration()
		config.initialize(args(0), sc.deployMode, args(1))
		val part = args(1).toInt
		
		config.print() 
		
		if (config.getMode == "local")
		{
			conf.setMaster("local[" + config.getNumTasks + "]")
			conf.set("spark.cores.max", config.getNumTasks)
		}
		else
		{
			conf.set("spark.shuffle.blockTransferService", "nio") 
			//conf.set("spark.network.timeout", "12000")
		}
	   
		val bcConfig = sc.broadcast(config)
		val hdfsManager = new HDFSManager
		
		if (part == 1)
		{
			if (config.getMode != "local")
			{
				hdfsManager.create("sparkLog.txt")
				hdfsManager.create("errorLog.txt")
			}
			else
			{
				val file = new File(config.getOutputFolder + "log")
				if (!file.exists)
					file.mkdir()
			}
		}
		
		var t0 = System.currentTimeMillis
		val numOfRegions = config.getNumRegions.toInt
		// Spark Listener
		/*sc.addSparkListener(new SparkListener() 
		{
			override def onApplicationStart(applicationStart: SparkListenerApplicationStart) 
			{
				statusLog("SparkListener:", t0, getTimeStamp() + " Spark ApplicationStart: " + applicationStart.appName + "\n", config)
			}

			override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) 
			{
				statusLog("SparkListener:", t0, getTimeStamp() + " Spark ApplicationEnd: " + applicationEnd.time + "\n", config)
			}

			override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) 
			{
				val map = stageCompleted.stageInfo.rddInfos
				map.foreach(row => {
					if (row.isCached)
					{	
						statusLog("SparkListener:", t0, getTimeStamp() + " " + row.name + ": memSize = " + (row.memSize / (1024*1024)) + 
								"MB, diskSize " + row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions, config)
					}
					else if (row.name.contains("rdd_"))
					{
						statusLog("SparkListener:", t0, getTimeStamp() + " " + row.name + " processed!", config)
					}
				})
			}
		});*/
		//////////////////////////////////////////////////////////////////////////
		if (part == 1)
		{ 
			val inputArray = getInputFileNames(config.getInputFolder, config).filter(x => x.contains(".fq")).map(x => x.replace(".gz", ""))  
			if (inputArray == null)
			{
				println("The input directory does not exist!")
				System.exit(1)
			}
			scala.util.Sorting.quickSort(inputArray)
			inputArray.foreach(println)
		
			// Give chunks to bwa instances
			val inputData = sc.parallelize(inputArray, inputArray.size) 
		
			// Run instances of bwa and get the output as Key Value pairs
			// <(chr, reg), fname>
			val bwaOutput = inputData.flatMap(x => bwaRun(x, bcConfig.value))
			var bwaOutStr = new StringBuilder
			for(e <- bwaOutput.collect)
			{
				val chr = e._1._1
				val reg = e._1._2
				val fname = e._2._1
				val reads = e._2._2
				val minPos = e._2._3
				val maxPos = e._2._4
				val posFname = e._2._5
			
				bwaOutStr.append(chr + "\t" + reg + "\t" + fname + "\t" + reads + "\t" + minPos + "\t" + maxPos + "\t" + posFname + "\n")
			}
			makeDirIfRequired(config.getOutputFolder + "bwaOut", config)
			writeWholeFile(config.getOutputFolder + "bwaOut.txt", bwaOutStr.toString, config)
		}
		else if (part == 2)
		{
			val input = ArrayBuffer.empty[((Integer, Integer), (String, Long, Int, Int, String))]
			val s = scala.collection.mutable.Set.empty[(Integer, Integer)]
			val inputLinesArray = readWholeFile(config.getOutputFolder + "bwaOut.txt", config).split('\n')
				
			for( x <- inputLinesArray)
			{
				val e = x.split('\t')
				input.append(((e(0).toInt, e(1).toInt), (e(2), e(3).toLong, e(4).toInt, e(5).toInt, e(6))))
				s.add((e(0).toInt, e(1).toInt))
			}
			
			// Hamid
			statusLog("Input Size: ", t0, input.size.toString, config)
			
			val inputArray = input.toArray
			// <(chr, reg), (fname, numOfReads, minPos, maxPos)>
			val inputData = sc.parallelize(inputArray, s.size)
			inputData.cache()
			val totalReads = inputData.map(x => x._2._2).reduce(_+_)
			// Hamid
			statusLog("Total Reads: ", t0, totalReads.toString, config)
			// <(chr, reg), Array((fname, numOfReads, minPos, maxPos))>
			val chrReg = inputData.groupByKey
			chrReg.cache()
			val avgReadsPerRegion = totalReads / chrReg.count
			// Hamid
			statusLog("chrReg: ", t0, "Chr regions:" + chrReg.count + ", Total reads: " + avgReadsPerRegion, config)
			// Previously
			// <(chr, reg), (fname, segments)>
			val loadBalRegions = 
			{
				if (!sizeBasedLBScheduling)
					chrReg.map(x => makeBAMFiles(x._1, x._2.toArray, avgReadsPerRegion, bcConfig.value))
				else
				{
					val chrRegByReads = chrReg.map(x => (x._1, x._2, x._2.map(_._2).reduce(_+_))).sortBy(_._3, false)
					chrRegByReads.map(x => makeBAMFiles(x._1, x._2.toArray, avgReadsPerRegion, bcConfig.value))
				}
			}
			//////////////////////////////////////////////////////////////////////
			val x = loadBalRegions.collect
			var regions1Str = new StringBuilder
			var segmentsStr = new StringBuilder
			for(e <- x)
			{
				val chr = e._1._1
				val reg = e._1._2
				val filesAndSegments = e._2
				
				if (!filesAndSegments.isEmpty)
				{
					segmentsStr.append(chr + "\t" + reg + "\t" + filesAndSegments(0)._2 + "\n")
					for (fs <- filesAndSegments)
						regions1Str.append(chr + "\t" + reg + "\t" + fs._1 + "\t" + fs._2 + "\n")
				}
			}
			
			writeWholeFile(config.getOutputFolder + "log/segments.txt", segmentsStr.toString, config)
		}
		else // (part == 3)
		{
			// For sorting
			implicit val vcfOrdering = new Ordering[(Integer, Integer)] {
				override def compare(a: (Integer, Integer), b: (Integer, Integer)) = if (a._1 == b._1) a._2 - b._2 else a._1 - b._1;
			}
			//
			
			var inputFileNames: Array[String] = null
			if (config.getMode != "local") 
				inputFileNames = getInputFileNames(config.getOutputFolder + "bed/", config).map(x => x.replace(".bed", ""))
			else 
				inputFileNames = getInputFileNames(config.getTmpFolder, config).filter(x => x.contains(".bed")).map(x => x.replace(".bed", ""))
			
			//////////////////////////////////////////////////////////////////////
			val inputData = 
			{
				if (!sizeBasedVCScheduling)
				{
					inputFileNames.foreach(println)
					sc.parallelize(inputFileNames, inputFileNames.size)
				}
				else
				{
					val inputFileNamesWithSize = inputFileNames.map(x => (x, hdfsManager.getFileSize(config.getOutputFolder + "bam/" + x + "-p1.bam")))
					val fileNamesBySize = inputFileNamesWithSize.sortWith(_._2 > _._2).map(_._1)
					fileNamesBySize.foreach(println)
					sc.parallelize(fileNamesBySize, fileNamesBySize.size)
				}
			}
			//////////////////////////////////////////////////////////////////////
			inputData.setName("rdd_inputData")
			val vcf = inputData.map(x => variantCall(x, bcConfig.value)).flatMap(x=> getVCF(x._1, bcConfig.value))
			vcf.setName("rdd_vcc")
			try
			{
				//vcf.distinct.sortByKey().map(_._2).coalesce(1, false).saveAsTextFile(config.getOutputFolder + "combinedVCF")
				val vcfCollected = vcf.distinct.sortByKey().map(_._2 + '\n').collect
				val writer = {
					if (config.getMode == "local")
						new PrintWriter(config.getOutputFolder + "sparkCombined.vcf")
					else
						hdfsManager.open(config.getOutputFolder + "sparkCombined.vcf")
				}
				for(e <- vcfCollected)
					writer.write(e)
				writer.close
			}
			finally 
			{
				// Close spark context
				sc.stop()
			}
		}
		//////////////////////////////////////////////////////////////////////////
		var et = (System.currentTimeMillis - t0) / 1000
		statusLog("Execution time:", t0, et.toString() + "\tsecs", config)
	}
	//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
