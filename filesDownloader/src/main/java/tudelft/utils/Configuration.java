/****************************************/
//	Class Name:	Configuration
//	Author:		Hamid Mushtaq  		
//	Company:	TU Delft	 	
/****************************************/
package tudelft.utils;

import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import java.io.Serializable;
import java.lang.System;

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
	private String gatkOpts;
	private String tmpFolder;
	private String hadoopInstall;
	private String numInstances;
	private String numRegions;
	private Long startTime;
	private String execMemGB;
	
	public void initialize(String configFile)
	{	
		try
		{
			File file = new File(configFile);
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			Document document = documentBuilder.parse(file);
			
			mode = document.getElementsByTagName("mode").item(0).getTextContent();
			refPath = document.getElementsByTagName("refPath").item(0).getTextContent();
			snpPath = document.getElementsByTagName("snpPath").item(0).getTextContent();
			indelPath = document.getElementsByTagName("indelPath").item(0).getTextContent();
			exomePath = document.getElementsByTagName("exomePath").item(0).getTextContent();
			inputFolder = correctFolderName(document.getElementsByTagName("inputFolder").item(0).getTextContent());
			outputFolder = correctFolderName(document.getElementsByTagName("outputFolder").item(0).getTextContent());
			toolsFolder = correctFolderName(document.getElementsByTagName("toolsFolder").item(0).getTextContent());
			rgString = document.getElementsByTagName("rgString").item(0).getTextContent();
			gatkOpts = document.getElementsByTagName("gatkOpts").item(0).getTextContent();
			tmpFolder = correctFolderName(document.getElementsByTagName("tmpFolder").item(0).getTextContent());
			hadoopInstall = correctFolderName(document.getElementsByTagName("hadoopInstall").item(0).getTextContent());
			numRegions = document.getElementsByTagName("numRegions").item(0).getTextContent();
			
			numInstances = document.getElementsByTagName("numNodes").item(0).getTextContent();
			execMemGB = document.getElementsByTagName("nodeMemoryGB").item(0).getTextContent();
	
			startTime = System.currentTimeMillis();
			
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
		return path.substring(path.lastIndexOf('/') + 1);
	}
	
	public String getMode()
	{
		return mode;
	}
	
	public String getRefPath()
	{
		return refPath;
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
	
	public String getNumInstances()
	{
		return numInstances;
	}
	
	public String getNumRegions()
	{
		return numRegions;
	}
	
	public void setNumInstances(String numInstances)
	{
		this.numInstances = numInstances;
	}
		
	public Long getStartTime()
	{
		return startTime;
	}
	
	public String getExecMemGB()
	{
		return execMemGB + "g";
	}
	
	public String getExecMemX()
	{
		Integer value = Integer.parseInt(execMemGB) * 1024;
		
		return "-Xmx" + value.toString() + "m";
	}
	
	public String getHadoopInstall()
	{
		return hadoopInstall;
	}
	
	public boolean useExome()
	{
		return !exomePath.trim().equals("");
	}
	
	public void print()
	{
		System.out.println("***** Configuration *****");
		System.out.println("Mode:\t\t" + "|" + mode + "|");
		System.out.println("Use exome = " + useExome());
		System.out.println("refPath:\t" + refPath);
		System.out.println("inputFolder:\t" + inputFolder);
		System.out.println("outputFolder:\t" + outputFolder);
		System.out.println("tmpFolder:\t" + tmpFolder);
		System.out.println("hadoopInstall:\t" + hadoopInstall);
		System.out.println("numInstances:\t" + numInstances);
		System.out.println("execMemGB:\t" + execMemGB);
		System.out.println("*************************");
	}
}