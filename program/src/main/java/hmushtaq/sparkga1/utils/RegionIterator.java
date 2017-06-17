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
package hmushtaq.sparkga1.utils;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import scala.Tuple2;
import java.util.ArrayList;
import java.io.*;

/**
 *
 * @author Hamid Mushtaq
 */
public class RegionIterator 
{
	protected class BedLine 
	{
		protected String chrName;
		protected int startPos;
		protected int endPos;

		protected BedLine(String chrName, int startPos, int endPos) 
		{
			this.chrName = chrName;
			this.startPos = startPos;
			this.endPos = endPos;
		}
        
		protected String getString() 
		{
			return chrName + "\t" + (startPos - 1)  + "\t" + (endPos - 1);
		}
	}
    
	protected ArrayList<BedLine> bedLinesList;
	protected SAMRecord sam = null;
	protected int reads = 0;
	protected int currentStart = -1, currentEnd = -1, currentChr = -1;
	protected String chrString = "";
	protected SAMFileHeader header;
	protected static final int INTERVAL_OVERLAP = 51;
	protected Tuple2<Integer, SAMRecord>[] samRecords;
	protected int index;
	protected int startIndex;
	protected int endIndex;

	public RegionIterator(Tuple2<Integer, SAMRecord>[] samRecords, SAMFileHeader header, int startIndex, int endIndex)
	{
		bedLinesList = new ArrayList<BedLine>();
		this.samRecords = samRecords;
		this.header = header;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		index = startIndex;
		getFirstRecord();
	}
    
	public boolean hasNext()
	{
		return (index < endIndex)? true : false;
	}
	
	public SAMRecord getNext()
	{
		return samRecords[index++]._2;
	}
	
	private void getFirstRecord() 
	{
		sam = null;
		if(hasNext()) 
		{
			sam = getNext();
			sam.setHeader(header);
			reads++;
			currentStart = sam.getAlignmentStart();
			currentEnd = sam.getAlignmentEnd();
			currentChr = sam.getReferenceIndex();
			chrString = sam.getReferenceName();
		}
	}

	public SAMRecord next() 
	{
		SAMRecord tmp = sam;
		if (hasNext()) 
		{
			sam = getNext();
			sam.setHeader(header);
			reads++;
			if(sam.getReferenceIndex() == currentChr && sam.getAlignmentStart() <= currentEnd + INTERVAL_OVERLAP)
			{
				if (sam.getAlignmentEnd() > currentEnd) 
					currentEnd = sam.getAlignmentEnd();
			} 
			else 
			{
				bedLinesList.add(new BedLine(chrString, currentStart, currentEnd));
				currentStart = sam.getAlignmentStart();
				currentEnd = sam.getAlignmentEnd();
				currentChr = sam.getReferenceIndex();
				chrString = sam.getReferenceName();
			}
		} 
		else 
			sam = null;
		return tmp;
	}

	public void addLastChrRange()
	{
		if (reads > 0)
			bedLinesList.add(new BedLine(chrString, currentStart, currentEnd));
	}
	
	public int getCount() 
	{
		return reads;
	}
    
	public void writeToBedFile(String bedFileName) throws IOException
	{        
		BufferedWriter bw = new BufferedWriter(new FileWriter(bedFileName));
		for(BedLine bedLine : bedLinesList) 
		{
			bw.write(bedLine.getString());
			bw.newLine();
		}
		bw.close();
	}
}
