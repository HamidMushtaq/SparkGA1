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
package tudelft.utils;

import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import java.io.Serializable;
import java.lang.System;

/**
 *
 * @author Hamid Mushtaq
 */
public class Configuration implements Serializable
{
	private String mode;
	private String refFileName;
	private String snpFileName;
	private String indelFileName;
	private String inputFolder;
	private String sfFolder;
	private String numNodes;
	
	public void initialize(String configFilePath, String deployMode)
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
			refFileName = document.getElementsByTagName("refFileName").item(0).getTextContent();
			snpFileName = document.getElementsByTagName("snpFileName").item(0).getTextContent();
			indelFileName = document.getElementsByTagName("indelFileName").item(0).getTextContent();
			inputFolder = correctFolderName(document.getElementsByTagName("inputFolder").item(0).getTextContent());
			sfFolder = correctFolderName(document.getElementsByTagName("sfFolder").item(0).getTextContent());
			numNodes = document.getElementsByTagName("numNodes").item(0).getTextContent();
			
			print();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private String correctFolderName(String s)
	{
		String r = s.trim();
		
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
	
	public String getMode()
	{
		return mode;
	}
	
	public String getRefFileName()
	{
		return refFileName;
	}
	
	public String getSnpFileName()
	{
		return snpFileName;
	}
	
	public String getIndelFileName()
	{
		return indelFileName;
	}
	
	public String getInputFolder()
	{
		return inputFolder;
	}
	
	public String getSfFolder()
	{
		return sfFolder;
	}
	
	public String getNumNodes()
	{
		return numNodes;
	}
	
	public void print()
	{
		System.out.println("***** Configuration *****");
		System.out.println("Mode:\t\t" + "|" + mode + "|");
		System.out.println("refFileName:\t" + refFileName);
		System.out.println("snpFileName:\t" + snpFileName);
		System.out.println("indelFileName:\t" + indelFileName);
		System.out.println("inputFolder:\t" + inputFolder);
		System.out.println("sfFolder:\t" + sfFolder);
		System.out.println("*************************");
	}
}