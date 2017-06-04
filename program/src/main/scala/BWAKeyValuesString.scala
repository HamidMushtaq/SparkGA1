import java.io.File
import java.io.InputStream
import java.io.FileInputStream
import java.util._
import htsjdk.samtools.util.BufferedLineReader
import htsjdk.samtools._
import tudelft.utils._
import tudelft.utils.Configuration

class BWAKeyValuesString(is: InputStream, config: Configuration)
{
	val keyValues = scala.collection.mutable.ArrayBuffer.empty[(Integer, SAMRecord)]
	val mConfig = config
	var mReads = 0
    val validationStringency: ValidationStringency = ValidationStringency.LENIENT;
    val mReader = new BufferedLineReader(is);
    val samRecordFactory = new DefaultSAMRecordFactory();
	private var mCurrentLine: String = null
	
	def getKeyValuePairs() : Array[(Integer, SAMRecord)] = 
	{
		return keyValues.toArray
	}
	
    def writeSAMRecord(sam: SAMRecord) : Integer = 
	{
        var count = 0
        val read1Ref = sam.getReferenceIndex()
		
		if (!sam.getReadUnmappedFlag() && (read1Ref >= 0) && (read1Ref <= 24))
		{
			val region = sam.getAlignmentStart / mConfig.getChrRegionSize(read1Ref)
		
			keyValues.append((read1Ref, sam))
			count = count + 1;
		}
		
		return count
    }
	
	def writeSAMRecord(sam: SAMRecord, 
		writerMap: scala.collection.mutable.HashMap[(Integer, Integer), scala.collection.mutable.ArrayBuffer[SAMRecord]]) : Integer = 
	{
        var count = 0
        val chr = sam.getReferenceIndex()
		
		if (!sam.getReadUnmappedFlag() && (chr >= 0) && (chr <= 24))
		{
			val reg = sam.getAlignmentStart / mConfig.getChrRegionSize(chr)
			
			if (!writerMap.contains((chr, reg)))
				writerMap += (((chr, reg), scala.collection.mutable.ArrayBuffer.empty[SAMRecord]))
			writerMap((chr, reg)).append(sam)
			
			count = count + 1
		}
		
		return count
    }
		
	def advanceLine() : String = 
    {
        mCurrentLine = mReader.readLine()
        return mCurrentLine;
    }
	
	def parseSam(writerMap: scala.collection.mutable.HashMap[(Integer, Integer), scala.collection.mutable.ArrayBuffer[SAMRecord]]) : Integer =  
	{
		var mParentReader: SAMFileReader = null
        val headerCodec = new SAMTextHeaderCodec();
        headerCodec.setValidationStringency(validationStringency)
        val mFileHeader = headerCodec.decode(mReader, null)
        val parser = new SAMLineParser(samRecordFactory, validationStringency, mFileHeader, null, null)
        // now process each read...
        var count = 0
		var badLines = 0
		
        mCurrentLine = mReader.readLine()
		
		if (mCurrentLine == null)
			println("Hamid >> mCurrentLine is null!")
		
		while (mCurrentLine != null) 
		{
			try
			{
			val samrecord = parser.parseLine(mCurrentLine, mReader.getLineNumber())
			
			if ((count % 500000) == 0)
				println("Hamid >> " + count + " records parsed.")
			
			if (writerMap == null)
				count += writeSAMRecord(samrecord)
			else
				count += writeSAMRecord(samrecord, writerMap)
			}
			catch
			{
				case e: Exception => badLines += 1
			}
			//advance line even if bad line
			advanceLine();
		}
        
		mReads = count
        println("SAMstream counts " + count + " records");
	
		return badLines
    }
	
	def getNumOfReads() : Integer =
	{
		return mReads
	}
	
	def close() =
	{
		mReader.close()
		is.close()
	}
}
