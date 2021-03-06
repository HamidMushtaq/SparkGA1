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

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import htsjdk.samtools.util.BufferedLineReader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import java.util.*;

/**
 *
 * @author Hamid Mushtaq
 */
public class DictParser
{
	SAMSequenceDictionary dict;
	ArrayList<Integer> chrLenArray;
	int[] chrRegionSizeArray;
	long chrLenSum;
	private HashMap<String, Integer> chrNameMap;
	private HashSet<String> ignoreListSet;
	
	public DictParser(HashSet<String> ilSet)
	{
		ignoreListSet = ilSet;
	}
	
	private String getLine(FileInputStream stream) throws IOException 
	{
		String tmp = "";
		int nlines = 0;
		try 
		{
			int c = stream.read();
			while(((char)c != '\n') && (c != -1)) 
			{
				tmp = tmp + (char)c;
				c = stream.read();
			}
			//System.err.println("|" + tmp + "|");
			return (c == -1)? null : tmp;
		} 
		catch (Exception ex) 
		{
			System.err.println("End of dict\n");
			return null;
		}
	}
	
	public long getChrLenSum()
	{
		return chrLenSum;
	}
	
	public ArrayList<Integer> getChrLenArray()
	{
		return chrLenArray;
	}
	
	void setChrRegionsSizes(int regions)
	{
		chrRegionSizeArray = new int[chrLenArray.size()];
		int avgRegionSize = (int)(chrLenSum / regions);
		
		for(int i = 0; i < chrLenArray.size(); i++)
		{
			int segments = chrLenArray.get(i) / avgRegionSize;
		
			if (segments == 0)
				segments = 1;
		
			chrRegionSizeArray[i] = chrLenArray.get(i) / segments;
			System.out.println("Hamid: chr" + i + " avgRegionSize = " + avgRegionSize + ", chrLen = " + chrLenArray.get(i));
			System.out.println("Hamid: chr" + i + " -> segments = " + segments + ", region size = " + chrRegionSizeArray[i]);
		}
	}
	
	public int[] getChrRegionSizeArray()
	{
		return chrRegionSizeArray;
	}
	
	public HashMap getChrNameMap()
	{
		return chrNameMap;
	}
		
	public SAMSequenceDictionary parse(String dictPath) 
	{
		try 
		{
			// Note: Change the file system according to the platform you are running on.
			FileInputStream stream = new FileInputStream(new File(dictPath));
			String line = getLine(stream); // header
			dict = new SAMSequenceDictionary();
			line = getLine(stream);
			chrLenArray = new ArrayList<Integer>();
			int chrIndex = 0;
			
			chrLenSum = 0;
			chrNameMap = new HashMap();
			while(line != null) 
			{
				// @SQ	SN:chrM	LN:16571
				String[] lineData = line.split("\\s+");
				String seqName = lineData[1].substring(lineData[1].indexOf(':') + 1);
				chrNameMap.put(seqName, chrIndex++);
				int seqLength = 0;
				try 
				{
					seqLength = Integer.parseInt(lineData[2].substring(lineData[2].indexOf(':') + 1));
					if (!ignoreListSet.contains(seqName))
					{
						chrLenSum += seqLength;
						chrLenArray.add(seqLength);
					}
				} 
				catch(NumberFormatException ex) 
				{
					System.out.println("Number format exception!\n");
				}
				SAMSequenceRecord seq = new SAMSequenceRecord(seqName, seqLength);
				dict.addSequence(seq);  
				line = getLine(stream);
			}
			stream.close();
			return dict;
		} 
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return null;
		}
	}
	
	public SAMSequenceDictionary getDict()
	{
		return dict;
	}
}