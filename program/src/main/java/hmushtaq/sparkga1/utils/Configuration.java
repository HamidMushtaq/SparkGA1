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

import htsjdk.samtools.*;
import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.*;
import java.io.Serializable;
import java.lang.System;
import java.util.*;

/**
 *
 * @author Hamid Mushtaq
 */
public class Configuration implements Serializable
{
	private String mode;
	private String refPath;
	private String snpPath;
	private String indelPath;
	private String exomePath;
	private String inputFolder;
	private String outputFolder;
	private String toolsFolder;
	private String rgString;
	private String extraBWAParams;
	private String gatkOpts;
	private String tmpFolder;
	private String sfFolder;
	private String numTasks;
	private String numInstance;
	private String numNodes;
	private String numThreads;
	private String ignoreList;
	private String numRegions;
	private String regionsFactor;
	private SAMSequenceDictionary dict;
	private String scc;
	private String sec;
	private String useKnownIndels;
	private Long startTime;
	private String driverMemGB;
	private String vcMemGB;
	private String chunkerConfigFilePath;
	private String chunkerGroupSize;
	private boolean streamingBWA;
	private boolean performIndelRealignment;
	private boolean performPrintReads;
	private ArrayList<Integer> chrLenArray;
	private int[] chrRegionSizeArray;
	private HashMap<String, Integer> chrNameMap;
	private HashSet<String> ignoreListSet;
	
	public void initialize(String configFilePath, String deployMode, String part)
	{	
		try
		{
			String configFile = configFilePath;
			if (deployMode.equals("cluster"))
				configFile = getFileNameFromPath(configFilePath);
			
			File file = new File(configFile);
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			Document document = documentBuilder.parse(file);
			
			mode = document.getElementsByTagName("mode").item(0).getTextContent();
			refPath = document.getElementsByTagName("refPath").item(0).getTextContent();
			snpPath = document.getElementsByTagName("snpPath").item(0).getTextContent();
			indelPath = emptyIfTagDoesntExist(document, "indelPath");
			exomePath = emptyIfTagDoesntExist(document, "exomePath");
			inputFolder = correctFolderName(document.getElementsByTagName("inputFolder").item(0).getTextContent());
			outputFolder = correctFolderName(document.getElementsByTagName("outputFolder").item(0).getTextContent());
			toolsFolder = correctFolderName(document.getElementsByTagName("toolsFolder").item(0).getTextContent());
			rgString = document.getElementsByTagName("rgString").item(0).getTextContent();
			extraBWAParams = document.getElementsByTagName("extraBWAParams").item(0).getTextContent();
			gatkOpts = emptyIfTagDoesntExist(document, "gatkOpts");
			tmpFolder = correctFolderName(document.getElementsByTagName("tmpFolder").item(0).getTextContent());
			sfFolder = correctFolderName(emptyIfTagDoesntExist(document, "sfFolder"));
			ignoreList = emptyIfTagDoesntExist(document, "ignoreList");
			//////////////////////////////////////////////////////////////////
			ignoreListSet = new HashSet<String>();
			String[] toIgnoreArray = ignoreList.trim().split(",");
			for(int i = 0; i < toIgnoreArray.length; i++)
			{
				String s = toIgnoreArray[i].trim();
				if (!s.equals(""))
					ignoreListSet.add(s);
			}
			//////////////////////////////////////////////////////////////////
			numRegions = document.getElementsByTagName("numRegions").item(0).getTextContent();
			regionsFactor = document.getElementsByTagName("regionsFactor").item(0).getTextContent();
			
			numTasks = document.getElementsByTagName("numTasks" + part).item(0).getTextContent();
			numThreads = document.getElementsByTagName("numThreads" + part).item(0).getTextContent();
			driverMemGB = document.getElementsByTagName("driverMemGB" + part).item(0).getTextContent();
			vcMemGB = document.getElementsByTagName("vcMemGB").item(0).getTextContent();
			scc	= document.getElementsByTagName("standCC").item(0).getTextContent();
			sec	= document.getElementsByTagName("standEC").item(0).getTextContent();
			sec	= document.getElementsByTagName("standEC").item(0).getTextContent();
			sec	= document.getElementsByTagName("standEC").item(0).getTextContent();
			
			performIndelRealignment = trueIfTagDoesntExist(document, "doIndelRealignment");
			performPrintReads = trueIfTagDoesntExist(document, "doPrintReads");
			chunkerConfigFilePath = emptyIfTagDoesntExist(document, "chunkerConfigFilePath");
			chunkerGroupSize = emptyIfTagDoesntExist(document, "chunkerGroupSize");
			
			streamingBWA = false;
			if (!chunkerConfigFilePath.equals(""))
				streamingBWA = true;
			
			if ( (!mode.equals("local")) && (!mode.equals("yarn-client")) && (!mode.equals("yarn-cluster")) )
				throw new IllegalArgumentException("Unrecognized mode type (" + mode + "). It should be either local, yarn-client or yarn-cluster.");
	
			startTime = System.currentTimeMillis();
			
			DictParser dictParser = new DictParser(ignoreListSet);
			if (mode.equals("local"))
			{
				String dictPath = refPath.replace(".fasta", ".dict");
				System.out.println("Parsing dictionary file " + dictPath);
				dict = dictParser.parse(dictPath);
			}
			else
			{
				System.out.println("Parsing dictionary file (cluster mode) ");
				dict = dictParser.parse(getFileNameFromPath(refPath).replace(".fasta", ".dict"));

				numInstance = document.getElementsByTagName("numInstances" + part).item(0).getTextContent();
				numNodes = document.getElementsByTagName("numNodes").item(0).getTextContent();
			}
			System.out.println("\n1.Hash code of dict = " + dict.hashCode() + "\n");
			chrLenArray = dictParser.getChrLenArray();
			dictParser.setChrRegionsSizes(Integer.parseInt(numRegions));
			chrRegionSizeArray = dictParser.getChrRegionSizeArray();
			chrNameMap = dictParser.getChrNameMap();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private boolean trueIfTagDoesntExist(Document document, String tag)
	{
		NodeList nl = document.getElementsByTagName(tag);
		if (nl.getLength() > 0)
		{
			String tagValue = nl.item(0).getTextContent();
			return Boolean.parseBoolean(tagValue);
		}
		else 
			return true;
	}
	
	private String emptyIfTagDoesntExist(Document document, String tag)
	{
		NodeList nl = document.getElementsByTagName(tag);
		if (nl.getLength() > 0)
			return nl.item(0).getTextContent();
		else 
			return "";
	}
	
	private String correctFolderName(String s)
	{
		String r = s.trim();
		
		if (r.equals(""))
			return "";
		
		if (r.charAt(r.length() - 1) != '/')
			return r + '/';
		else
			return r;
	}
	
	private String getFileNameFromPath(String path)
	{
		if (path.contains("/"))
			return path.substring(path.lastIndexOf('/') + 1);
		else
			return path;
	}
	
	public SAMSequenceDictionary getDict()
	{
		return dict;
	}
	
	public boolean isInIgnoreList(String s)
	{
		return ignoreListSet.contains(s);
	}
	
	public int getChrLen(int chr)
	{
		return chrLenArray.get(chr);
	}
	
	public int getChrRegionSize(int chr)
	{
		return chrRegionSizeArray[chr];
	}

	public String getMode()
	{
		return mode;
	}

	public String getNumInstance(){
		return numInstance;
	}

	public String getNumNodes(){
		return numNodes;
	}
	
	public String getRefPath()
	{
		return refPath;
	}
	
	public String getDictPath()
	{
		return refPath.replace(".fasta", ".dict");
	}
	
	public String getSnpPath()
	{
		return snpPath;
	}
	
	public String getIndelPath()
	{
		return indelPath;
	}
	
	public String getExomePath()
	{
		return exomePath;
	}
	
	public String getInputFolder()
	{
		return inputFolder;
	}
	
	public String getOutputFolder()
	{
		return outputFolder;
	}
	
	public String getToolsFolder()
	{
		return toolsFolder;
	}
	
	public String getRGString()
	{
		return rgString;
	}
	
	public String getExtraBWAParams()
	{
		return extraBWAParams;
	}
	
	public String getRGID()
	{
		int start = rgString.indexOf("ID:");
		int end = rgString.indexOf("\\", start);
		
		return rgString.substring(start+3, end);
	}
	
	public String getGATKopts()
	{
		return gatkOpts;
	}
	
	public String getTmpFolder()
	{
		return tmpFolder;
	}
	
	public String getSfFolder()
	{
		return sfFolder;
	}
	
	public String getNumTasks()
	{
		return numTasks;
	}
	
	public String getNumThreads()
	{
		return numThreads;
	}
	
	public String getIgnoreList()
	{
		return ignoreList;
	}
	
	public String getNumRegions()
	{
		return numRegions;
	}
	
	public void setNumThreads(String numThreads)
	{
		this.numThreads = numThreads;
	}

	public String getSCC()
	{
		return scc.toString();
	}
	
	public String getSEC()
	{
		return sec.toString();
	}
		
	public Long getStartTime()
	{
		return startTime;
	}
	
	public String getDriverMemGB()
	{
		return driverMemGB + "g";
	}
	
	public String getExecMemX()
	{
		Integer execValue = Integer.parseInt(vcMemGB) * 1024; 
		
		return "-Xmx" + execValue.toString() + "m";
	}
	
	public String getChunkerGroupSize()
	{
		return chunkerGroupSize;
	}
	
	public boolean useExome()
	{
		return !exomePath.trim().equals("");
	}
	
	public boolean useKnownIndels()
	{
		return !indelPath.trim().equals("");
	}
	
	public boolean doIndelRealignment()
	{
		return performIndelRealignment;
	}
	
	public boolean doPrintReads()
	{
		return performPrintReads;
	}
	
	public boolean doStreamingBWA()
	{
		return streamingBWA;
	}
	
	public int getChrIndex(String chrName)
	{
		return chrNameMap.get(chrName);
	}
	
	public String getRegionsFactor()
	{
		return regionsFactor;
	}
	
	public void print()
	{
		System.out.println("***** Configuration *****");
		System.out.println("Mode:\t\t" + "|" + mode + "|");
		System.out.println("Use exome = |" + useExome() + "|");
		System.out.println("refPath:\t|" + refPath + "|");
		System.out.println("inputFolder:\t|" + inputFolder + "|");
		System.out.println("outputFolder:\t|" + outputFolder + "|");
		System.out.println("tmpFolder:\t|" + tmpFolder + "|");
		System.out.println("sfFolder:\t|" + sfFolder + "|");
		System.out.println("ignoreList:\t|" + ignoreList + "|");
		System.out.println("numTasks:\t|" + numTasks + "|");
		System.out.println("numThreads:\t|" + numThreads + "|");
		System.out.println("driverMemGB:\t|" + driverMemGB + "|");
		System.out.println("streamingBWA:\t|" + streamingBWA + "|");
		System.out.println("chunkerGroupSize:\t|" + chunkerGroupSize + "|");
		System.out.println("doIndelRealignment:\t|" + performIndelRealignment + "|");
		System.out.println("doPrintReads:\t|" + performPrintReads + "|");
		for (String key : chrNameMap.keySet()) {
			System.out.println("\tChromosome " + key + " -> " + chrNameMap.get(key)); 
		}
		System.out.println("-------------------------");
		System.out.println("ignoreList:");
		for (String s: ignoreListSet)
			System.out.println("<" + s + ">");
		System.out.println("*************************");
	}
}