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
package hmushtaq.sparkga1.utils

import java.io._
import java.nio.file.Files
import java.nio.file.Paths
import htsjdk.samtools.SAMRecord
import scala.collection.mutable._

/**
 *
 * @author Hamid Mushtaq
 */
class SamRegion(header: String, fileName: String, config: Configuration)
{
	private final val USE_FILE = true
	private var minPos = 0
	private var maxPos = 0
	private var size: Long = 0
	private var sbPos = new StringBuilder
	private var contentWriter: Writer = {
		if (USE_FILE) 
			new PrintWriter(config.getTmpFolder + fileName)
		else 
			new StringWriter
	}
	
	contentWriter.write(header)

	def append(chrPos: Int, line: String) = 
	{
		size += 1
		sbPos.append(chrPos + "\n")
		contentWriter.write(line + "\n")
		
		if (maxPos == 0)
		{
			minPos = chrPos
			maxPos = chrPos
		}
		
		if (chrPos < minPos)
			minPos = chrPos
		
		if (chrPos > maxPos)
			maxPos = chrPos
	}
	
	def getSize: Long =
	{
		return size
	}
	
	def getMinPos: Int =
	{
		return minPos
	}
	
	def getMaxPos: Int = 
	{
		return maxPos
	}
	
	def getContent: String =
	{
		contentWriter.close
		var content: String = null
		
		if (USE_FILE)
		{
			content = new String(Files.readAllBytes(Paths.get(config.getTmpFolder + fileName))) 
			new File(config.getTmpFolder + fileName).delete
		}
		else
			content = contentWriter.toString
		
		contentWriter = null
		return content
	}
	
	def getPositionsStr: String =
	{
		val positionsStr = sbPos.toString
		sbPos = null
		return positionsStr
	}
}
