import tudelft.utils.Configuration

class SamRegionsParser(chunkID: String, writerMap: scala.collection.mutable.HashMap[(Integer, Integer), SamRegion], config: Configuration)
{
	var mReads = 0
	var badLines = 0
	val header = new StringBuilder
	
	def append(line: String) : Integer = 
	{
		if (line(0) == '@')
		{
			header.append(line + '\n')
			return 0
		}
		
		try
		{
			val fields = line.split('\t')
			val flags = fields(1).toInt
			
			// Hamid: If read is unmapped 
			if ((flags & 4) > 0)
				return 1
		
			if (fields(2) == "*")
				return 1
				
			val chr = config.getChrIndex(fields(2))
			val chrPos = fields(3).toInt
			
			if ((chr >= 0) && (chr <= 24))
			{
				val reg = chrPos / config.getChrRegionSize(chr)
				
				if (!writerMap.contains((chr, reg)))
					writerMap.put((chr, reg), new SamRegion(header.toString, chr + "_" + reg + "_" + chunkID, config))
				writerMap((chr, reg)).append(chrPos, line)
				
				mReads += 1
			}
		
			return 1
		}
		catch
		{
			case e: Exception => println("badline<" + line + ">"); badLines += 1; return -1
		}
    }
		
	def getNumOfReads() : Integer =
	{
		return mReads
	}
	
	def getBadLines(): Integer = 
	{
		return badLines
	}
}
