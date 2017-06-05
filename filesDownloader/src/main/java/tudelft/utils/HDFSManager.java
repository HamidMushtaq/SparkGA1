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
package tudelft.utils;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.net.*;
import java.lang.*;

public class HDFSManager
{
	public static void create(String hadoopInstall, String fname)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				fs.delete(filenamePath, true);
				
			fs = FileSystem.get(config);
			FSDataOutputStream fout = fs.create(filenamePath);
			fout.close();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public static void append(String hadoopInstall, String fname, String s)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{			
			FSDataOutputStream fout = FileSystem.get(config).append(new Path(fname));
			PrintWriter writer = new PrintWriter(fout);
			writer.append(s);
			writer.close();
			fout.close();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public static String getLS(String dir, boolean showHidden)
	{
		try
		{
			File folder = new File(dir);
			File[] listOfFiles = folder.listFiles();
			String lenStr = Integer.toString(listOfFiles.length);
			String str = "";
			
			InetAddress IP = InetAddress.getLocalHost();
			String hostName = IP.toString();
			
			for (int i = 0; i < listOfFiles.length; i++) 
			{
				if (listOfFiles[i].isFile())
				{
					if (!(listOfFiles[i].isHidden() && !showHidden))
						str = str + String.format("%s\t%s\n", getSizeString(listOfFiles[i].length()),
							listOfFiles[i].getName());
				}
				else if (listOfFiles[i].isDirectory()) 
					str = str + "DIR:\t" + listOfFiles[i].getName() + "\n";
			}
			return "\n************\n\tNumber of files in " + dir + " of " + hostName + 
				" = " + lenStr + "\n" + str + "\n************\n";
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return "Exception in HDFSManager.getLS";
		}
	}
	
	public static int download(String hadoopInstall, String fileName, String hdfsFolder, String localFolder)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			
			File f = new File(localFolder + fileName);
			if (f.exists())
				f.delete();
			
			fs.copyToLocalFile(new Path(hdfsFolder + fileName), 
				new Path(localFolder + fileName));
			return 1;
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return 0;
		}
	}
	
	public static void upload(String hadoopInstall, String fileName, String localFolder, String hdfsFolder)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			
			fs.copyFromLocalFile(true, true, new Path(localFolder + fileName), 
				new Path(hdfsFolder + fileName));
		}
		catch (Exception ex) 
		{
			ex.printStackTrace();
		}
	}

	public static String[] getFileList(String hadoopInstall, String hdfsFolder)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{
			FileSystem fs = FileSystem.get(config);
			FileStatus[] status = fs.listStatus(new Path(hdfsFolder));
			String[] fileNames = new String[status.length];

			for (int i=0; i < status.length; i++)
				fileNames[i] = status[i].getPath().getName();

			return fileNames;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return null;
		}
	}
	
	private static String getSizeString(long len)
	{
		Float r;
		String unit;
		
		if (len > 1e9)
		{
			r = len / 1e9f;
			unit = "GB";
		}
		else if (len > 1e6)
		{
			r = len / 1e6f;
			unit = "MB";
		}
		else
		{
			r = len / 1e3f;
			unit = "KB";
		}
		
		return String.format("%.1f%s", r, unit);
	}
}