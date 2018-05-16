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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.net.*;
import java.lang.*;
import java.nio.charset.Charset;

/**
 *
 * @author Hamid Mushtaq
 */
public class HDFSManager
{
	private final static boolean DISABLE_CACHE = false;
	private org.apache.hadoop.conf.Configuration config;
	private FileSystem fs;
	
	public HDFSManager() throws IOException
	{
		config = new org.apache.hadoop.conf.Configuration();
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		fs = FileSystem.get(config);
	}
	
	public void create(String fname) throws IOException
	{
		try
		{
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				fs.delete(filenamePath, true);
				
			FSDataOutputStream fout = fs.create(filenamePath);
			fout.close();
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
		}
	}

	public void createIfRequired(String fname) throws IOException{
		try
		{
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				return;
				
			FSDataOutputStream fout = fs.create(filenamePath);
			fout.close();
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
		}
	}
	
	public PrintWriter open(String fname) throws IOException
	{
		try
		{
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				fs.delete(filenamePath, true);
				
			FSDataOutputStream fout = fs.create(filenamePath);
			Charset UTF8 = Charset.forName("utf-8");
			PrintWriter writer = new PrintWriter(new OutputStreamWriter(fout, UTF8));
			return writer;
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return null;
		}
	}
	
	public OutputStream openStream(String fname) throws IOException
	{
		try
		{
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				fs.delete(filenamePath, true);
				
			FSDataOutputStream fout = fs.create(filenamePath);
			return fout;
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return null;
		}
	}
	
	public boolean exists(String fname)
	{
		try
		{
			Path filenamePath = new Path(fname);  
		
			return fs.exists(filenamePath);		
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return false;
		}
	}
	
	/*public void append(String fname, String s)
	{
		try
		{			
			FSDataOutputStream fout = fs.append(new Path(fname));
			PrintWriter writer = new PrintWriter(fout);
			writer.append(s);
			writer.close();
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
		}
	}*/
	public void append(String fname, String s)
	{
		try
		{
			Path filenamePath = new Path(fname);
			if (!fs.exists(filenamePath))
			{
				FSDataOutputStream fout = fs.create(filenamePath);
				fout.close();
			}
			writeWholeFile(fname, readWholeFile(fname) + s);
		}
		catch (IOException ex)
		{
			ex.printStackTrace();
		}
	}
	
	public String readWholeFile(String fname)
	{
		try
		{			
			StringBuilder builder=new StringBuilder();
			byte[] buffer=new byte[8192000];
			int bytesRead;
  
			FSDataInputStream in = fs.open(new Path(fname));
			while ((bytesRead = in.read(buffer)) > 0) 
				builder.append(new String(buffer, 0, bytesRead, "UTF-8"));
			in.close();
			
			return builder.toString();
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return "";
		}
	}
	
	public byte[] readBytes(String fname)
	{
		try
		{			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] buffer=new byte[67108864];
			int bytesRead;
  
			FSDataInputStream in = fs.open(new Path(fname));
			while ((bytesRead = in.read(buffer)) > 0) 
				baos.write(buffer, 0, bytesRead);
			in.close();
			
			return baos.toByteArray();
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return new byte[0];
		}
	}
	
	public String readPartialFile(String fname, int bytes)
	{
		try
		{			
			StringBuilder builder=new StringBuilder();
			ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes+1);
			
			FSDataInputStream in = fs.open(new Path(fname));
			IOUtils.copyBytes(in, baos, bytes+1, false);
			in.close();
			
			return new String(baos.toString("UTF-8"));
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return "IOException!";
		}
	}
	
	public void writeWholeFile(String fname, String s)
	{
		try
		{
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				fs.delete(filenamePath, true);
				
			FSDataOutputStream fout = fs.create(filenamePath);
			Charset UTF8 = Charset.forName("utf-8");
			PrintWriter writer = new PrintWriter(new OutputStreamWriter(fout, UTF8));
			writer.write(s);
			writer.flush();
			writer.close();
			fout.close();
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
		}
	}
	
	public void writeBytes(String fname, byte[] bytes)
	{
		try
		{
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				fs.delete(filenamePath, true);
				
			FSDataOutputStream fout = fs.create(filenamePath);
			fout.write(bytes);
			fout.close();
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
		}
	}
	
	public void remove(String fname)
	{
		try
		{			
			fs.delete(new Path(fname), true);
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
		}
	}
	
	public int download(String fileName, String hdfsFolder, String localFolder, boolean overwrite)
	{
		try
		{
			File f = new File(localFolder + fileName);
			if (f.exists() && !overwrite)
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
	
	public int downloadIfRequired(String fileName, String hdfsFolder, String localFolder)
	{
		try
		{	
			File f = new File(localFolder + fileName);
			if (!f.exists())
				fs.copyToLocalFile(new Path(hdfsFolder + fileName), new Path(localFolder + fileName));
			else
			{
				// Some other thread is already copying this file.
				long fileLen = f.length();
				while(fileLen != getFileSize(hdfsFolder + fileName))
				{
					Thread.sleep(1000);
					fileLen = f.length();
				}
			}
			
			return 1;
		}
		catch (Exception ex) 
		{
			ex.printStackTrace();
			return 0;
		}
	}
	
	public long getFileSize(String filePath)
	{
		try
		{
			Path path = new Path(filePath);  
			
			if (!fs.exists(path))
				return -1;
			else
				return fs.getFileStatus(path).getLen();
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return -2;
		}
	}
	
	public void upload(String fileName, String localFolder, String hdfsFolder, boolean delSrc)
	{
		try
		{	
			fs.copyFromLocalFile(delSrc, true, new Path(localFolder + fileName), 
				new Path(hdfsFolder + fileName));
		}
		catch (Exception ex) 
		{
			ex.printStackTrace();
		}
	}

	public void uploadFolder(String localFolder, String hdfsFolder, boolean delSrc){
		try
		{	
			fs.copyFromLocalFile(delSrc, true, new Path(localFolder), new Path(hdfsFolder));
		}
		catch (Exception ex) 
		{
			ex.printStackTrace();
		}
	}

	public String[] getFileList(String hdfsFolder)
	{
		try
		{
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
}