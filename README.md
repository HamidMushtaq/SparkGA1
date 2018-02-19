# SparkGA

SparkGA is an Apache Spark based scalable implementation of the best practice DNA analysis pipeline by Broad Institute. A paper describing its implementation was accepted for the ACM-BCB 2017 conference. So, if you use SparkGA for your work, please cite the following paper.

> Hamid Mushtaq, Frank Liu, Carlos Costa, Gang Liu, Peter Hofstee, and Zaid
Al-Ars. 2017. SparkGA: A Spark Framework for Cost Effective, Fast and
Accurate DNA Analysis at Scale. In Proceedings of ACM-BCB ’17, Boston,
MA, USA, August 20-23, 2017.

This README contains the following sections.
1. **System requirements and Installation**
2. **Compiling**
3. **Files required**
4. **Putting reference and index files on each node of the cluster**
5. **Configuration files**
	* **Configuration file for SparkGA**
	* **Configuration file for the chunker utility**
	* **Configuration file for the downloader utility**
6. **Running SparkGA**

## System requirements and Installation
For SparkGA, you must have the following software packages installed in your system/cluster. Note that SparkGA runs on a yarn cluster, so when running in cluster mode, the cluster should have been properly setup. Moreover, your system/cluster must be Linux-based.

* Java 1.7 or greater
* Hadoop 2.4 or greater
* Apache Spark 2.0.0 or greater

Note that your Spark version should match the hadoop version. For example, if you have hadoop version 2.5.1, you must use spark-2.x.x-bin-hadoop2.4. Moreover you must have an evironment variable ```$SPARK_HOME``` that points to the Spark directory. When running in cluster mode (yarn-client or yarn-cluster), you must add the following to ```$SPARK_HOME/conf/spark-env.sh```,

> HADOOP_CONF_DIR=$HADOOP_INSTALL/etc/hadoop

This of course assumes that you have an environment variable $HADOOP_INSTALL that points to the hadoop's home directory.

## Compiling
For compiling, go to the program folder and type `sbt package`. Likewise, for compiling the filesDownloader utility, go to the filesDownloader folder and type `sbt package`. The code for the chunker utility can be found at https://github.com/HamidMushtaq/FastqChunker, but a compiled jar file is also placed in the chunker folder here.

## Files required

Before you run SparkGA, make sure you have the following files available.

1. The input xml file which contains configuration parameters for running SparkGA. Each tag for that xml file is explained in Section **Configuration file for SparkGA**.

2. All the reference and index files. This means files such as the fasta, bwt, dict and known snp vcf files etc. These are the same files that are required for the classical GATK pipeline from broad. These files are required to be available in a folder or in the HDFS, depending upon whether you are running SparkGA in local or cluster (yarn-client or yarn-cluster) mode. For cluster mode, you can put these files on the local directories of each node in three different ways. This is explained later in this document in Section **Putting reference and index files on each node of the cluster**. Note that the .dict file should also be present in SparkGA'S main folder. 

3. The input FASTQ files, either in compressed (gzipped) or uncompressed form. For a single-ended input, only one FASTQ file is requied, while for pair-ended input, two files are required. The chunker program can be used to make chunks or interleaved chunks (for paired-ended input) from the input files. However, this chunking can be done in parallel with the execution of SparkGA, by simply adding two extra parameters in the input xml file. More on this is described later in this document. When chunking is done in parallel, SparkGA will start execution as soon as the first chunks are made available by the chunker program. In this way, the chunker program could be uploading the chunks on the fly.

4. All the tools in one directory. This path of this directory is specified as the value of ```toolsFolder``` in the input xml file. The following tools should be in the ```toolsFolder``` directory.
	* bwa 0.7.x
	* bedtools 2.x
	* Picard's Mardkuplicates.jar and CleanSam.jar, version 1.11x
	* GATK's GenomeAnalysisTK.jar, version 3.x

5. In SparkGA'a main folder, you must have the file ```lib/htsjdk-1.143```. Besides that, to run the SparkGA program, you must use the python scripts ```runPart.py``` (to run each part, which are mapping, dynamic load balancing and variant calling, separately) or ```runAll.py``` (to run all the parts one after another). These scripts should be in SparkGA's main folder. Note that the ```runAll.py``` script internally uses ```runPart.py``` to run all the parts one after another. More details are given in the section **Running SparkGA**.

6. In SparkGA's main folder, you must have the main program ```sparkga1_2.11-1.0.jar``` in a folder named ```program```.

7. The chunker program ```chunker_2.11-1.0.jar``` should be in the folder ```chunker``` inside the SparkGA's main folder. If you want to make the chunks separately (not on the fly), you must use the the python script ```runChunker.py``` in SparkGA's main folder. In either case, you also need a separate xml configuration file for the chunker program. Each tag for such xml file is explained in Section **Configuration file for the chunker utility**.

8. If you are using the downloader utility (More on the downloader utility later in the text), you must have the downloader program ```filesdownloader_2.11-1.0.jar``` in the folder ```filesDownloader```. To run the downloader utility, you must use the python script ```runDownloader.py``` in SparkGA's main folder. Like SparkGA's main program and the chunker utility, the downloader utility requires an input xml file, which is explained in Section **Configuration file for the downloader utility**.

## Putting reference and index files on each node of the cluster
This can be done in three different ways, as listed below.

1. The first obvious way is to manually place these files in some local directory of each node.
2. Another way is to give the value `./` to the `sfFolder` field of SparkGA's configuration file. Giving this value means that these files should be in an executor's current directory. Therefore SparkGA will itself download them from the HDFS.
3. The third way is to use the filesDownloader utility, which will copy these files on each node. The filesDownloader utility takes an xml file as input (Please see Section **Configuration file for the downloader utility** for more details) and can be run using the `runDownloader.py` python script. This script takes at least two parameters. The first parameter is the path of the xml file, while the second parameter is the mode. The mode can be one of the following. Note that for some modes (For example, **cp**) a third parameter is required.
	* **cpAll**: Copy all the reference and index files to the local directory (sfFolder) of all nodes
	* **cp**: Copy a file (name of file is given as the third parameter) from the input hdfs folder to the local directory (sfFolder) of all nodes
	* **rmAll**: Remove all files from the local directory (sfFolder) of all nodes
	* **rmJunk**: Remove junk files, that is, every file besides the referenced and index files, from the local directory (sfFolder) of all nodes.
	* **rmExt**: Remove files of a certain extension (extension is given as the third parameter) from the local directory directory (sfFolder) of all nodes
	* **rm:** Remove a file (name of file is given as the third parameter) from the local directory (sfFolder) of all nodes
	* **ls**: Display files in the local directory (sfFolder) of all nodes

## Configuration files

### Configuration file for SparkGA
Three example configuration files are given in the `config `folder, namely `localSingleEndedExample.xml`, `pairedWithIgnoreExample.xml` and `singleEndedExample.xml`.

1. **mode** - The mode of running. Its value could be `local`, `yarn-client` or `yarn-cluster` (we will collectively call `yarn-client` and `yarn-cluster`, cluster modes from now on).
2. **refPath** - The path of the FASTA file. All the accompanying files, such as \*.fasta.bwt, \*.dict and \*.fasta.pac etc. should be in the same folder as the FASTA file. The names of those files would be infered from the FASTA file. For local mode, these files should be in some local directory, while for cluster modes, they should be in an HDFS directory. Note that if you have already downloaded these files to each node, then only giving the name of the fasta file would suffice. This rule also applies for ```snpPath``` and ```indelPath```.
3. **snpPath** - The path to the snp vcf file used for known sites in base recalibration. 
4. **indelPath** - The path to the indel vcf file used for known indels in indel realignment. If you want the indel realigner to not use any known indels, you can leave this field empty, or even ommit this tag altogehter.
5. **exomePath** - The path of the bed or intervals file for exome sequencing. For local mode, this should be a local path, while for cluster modes an HDFS path. Note that in cluster modes, this file is read directly from the HDFS. Therefore, this file must be present in the HDFS for cluster modes. For whole genome, when you don't require an exome file, you can leave this field empty or even ommit it.
6. **inputFolder** - The folder that contains the input chunks. For local mode, this would be a local directory, while for cluster modes, an HDFS directory. If you specify `chunkerConfigFilePath` and `chunkerGroupSize` (See tags at number 37 and 38), the chunker utility would create this folder and upload chunks on the fly.
7. **outputFolder** - The output folder that will contain the final output vcf file namely `sparkCombined.vcf`. Moreover, this folder would also contain the log folder with log files, the purpose of which is to let the user to see the progress of the program. 
8. **toolsFolder** - This folder contains all the tools for executing the GATK pipeline, such as bwa, Markduplicates.jar and GenomeAnalysiTK.jar etc. This folder would always be in a local directory, regardless of the mode. For cluster modes, the python script `runPart.py` would send the programs to each executor, using `--files` when executing `spark-submit`.
9. **tmpFolder** - This folder is the one used for storing temporary files. This folder would be in a local directory. For cluster modes, this folder would be present on each node.
10. **sfFolder** - This is the folder, where all the reference and index files are stored. This folder would be in a local directory. For cluster modes, this folder would be present on each node. If `./` is used, SparkGA would download the reference files to `./`, which is executor's current directory. This means that those files would also get deleted at the end of execution. To permanently place these files on the local directories of nodes, either use the filesDownloader utility or manually copy them to each node.
11. **rgString** - The read group string given to the bwa program
12. **extraBWAParams** - Any extra parameters given to the bwa program besides the read group string, input files, reference fasta file name and number of threads. For example, for paired-ended reads, you must put `-p`, as the chunker utility would always create interleaved chunks for paired-ended FASTQ files.
13. **gatkOpts** - Any additional parameters given when running jar files. For example, `-XX:+AggressiveOpts`.
14. **ignoreList** - Name of Chromosomes (as given in the reference dictionary file) that have to be ignored. The list should be comma separated. White spaces characters such as newlines, spaces and tabs are ignored.
15. **numRegions** - The number of regions to create for variant calling. The number of regions created by the second part (dynamic load balancing) would be not exact though, but just an approximate.
16. **regionsFactor** - The number of regions would be multiplied by this factor. We have found that the ideal value of numRegions is around 300, as increasing its value would increase HDFS access overhead. If you want more regions than that, then use `regionsFactor`. The total number of regions created would then be `numRegions * regionsFactor.`
17. **execMemGB1** - The executor memory in GB for Part 1. Note that this parameter doesn't make sense for local mode. So, for local mode, you can even ommit it.
18. **driverMemGB1** - The driver memory in GB for Part 1. For local mode, if say you want to use the full memory of the system, then the driver memory should be nearly equal to the full memory of the system. For cluster modes however, this is only the memory of the driver.
19. **numInstances1** - The number of instances of executors for Part 1. Note that this parameter doesn't make sense for local mode. So, for local mode, you can even ommit it.
20. **numThreads1** - The number of threads to use for bwa in Part 1.
21. **numTasks1** - The number of tasks per each executor in part1. In cluster modes, the total number of tasks running on the whole cluster would then be `numInstances1 * numTasks1`. For local mode, on the other hand, the total number of tasks would simply be equal to `numTasks1`.
22. **execMemGB2** - The executor memory in GB for Part 2. Note that this parameter doesn't make sense for local mode. So, for local mode, you can even ommit it.
23. **driverMemGB2** - The driver memory in GB for Part 2. For local mode, if say you want to use the full memory of the system, then the driver memory should be nearly equal to the full memory of the system. For cluster modes however, this is only the memory of the driver.
24. **numInstances2** - The number of instances of executors for Part 2. Note that this parameter doesn't make sense for local mode. So, for local mode, you can even ommit it.
25. **numThreads2** - The maximum number of threads to use for dynamic load balancing in Part 2.
26. **numTasks2** - The number of tasks per each executor in Part 2. In cluster modes, the total number of tasks running on the whole cluster would then be `numInstances2 * numTasks2`. For local mode, on the other hand, the total number of tasks would simply be equal to `numTasks2`.
27. **execMemGB3** - The executor memory in GB for Part 3. Note that this parameter doesn't make sense for local mode. So, for local mode, you can even ommit it.
28. **vcMemGB** - The memory in GB used by Java to run a jar program.
29. **driverMemGB3** - The driver memory in GB for Part 3. For local mode, if say you want to use the full memory of the system, then the driver memory should be nearly equal to the full memory of the system. For cluster modes however, this is only the memory of the driver.
30. **numInstances3** - The number of instances of executors for Part 3. Note that this parameter doesn't make sense for local mode. So, for local mode, you can even ommit it.
31. **numThreads3** - The number of threads to use for variant discovery tools of Part 3. We have noticed that for haplotype caller, for regions with a lot of mutations, increasing the number of threads greatly improve the performance. When running on a machine with simultaneous multithreading, at least keep the number of threads equal to the number of simultaneous threads of a core.
32. **numTasks3** - The number of tasks per each executor in Part 3. In cluster modes, the total number of tasks running on the whole cluster would then be `numInstances3 * numTasks3`. For local mode, on the other hand, the total number of tasks would simply be equal to `numTasks3`. 
33. **standEC** - The value of `stand_emit_conf` for haplotype caller. For GATK 3.7+, keep this value 0.
34. **standCC** - The value of `stand_call_conf` for haplotype caller. For GATK 3.7+, keep this value 0.
35. **doIndelRealignment** - `true `or `false` depending on whether you want to perform indel realignment. Assumed `true` if ommitted.
36. **doPrintReads** - `true `or `false` depending on whether you want to perform print reads. Assumed `true` if ommitted.
37. **chunkerConfigFilePath** - If you want to perform chunking of the input FASTQ files on the fly, give the path of the config file for the chunker utility. If you leave this field empty or ommit it, chunking would not be done on the fly. 
38. **chunkerGroupSize** - When chunking is done in parallel, the Part 1 of SparkGA would process chunks in groups. For example, if you give a group size of 512, and the chunker eventually creates 1500 chunks overall, Part 1 would first process the 512 files and when all those 512 files are processed, will start processing the next 512 chunks. So, it would complete execution in 3 steps. However, if you can accurately predict the number of chunks that would be created by the chunker, you can give a slightly higher estimate. For example, if you give a value of 1700 for our example, Part 1 would just use one round. The tasks processing chunk IDs greater than 1499 would simply return without doing anything, as no correspodnding chunk would exist for such IDs. Therefore, giving a slightly higher estimate won't impact the execution speed of Part 1 as such. If you are not doing chunking in parallel, then of course you can leave this field empty or even ommit it altogether.

### Configuration file for the chunker utility
The chunker program is run on the master node, which means it is not distributed like SparkGA. It can take both compressed (gzipped) or uncompressed FASTQ files as input. By default, it uploads the chunks it makes to an HDFS directory specified as `outputFolder` in the configuration file. However, if you prefix the value of `outputFolder` with `local:`, it would put the chunks in a local folder. For example, if you specify `local:/home/hamidmushtaq/data/chunks`, it would put the chunks in the folder `/home/hamidmushtaq/data/chunks`. So, when using with SparkGA in local mode, always use the prefix `local:` for the `outputFolder` in the configuration file of the chunker utility. Moreover, you don't have to create the output folder yourself, as the chunker program would create one itself. Two example configuration files are shown in the `config/chunkerConfig folder`.

1. **fastq1Path** - The path of the first FASTQ file for a pair-ended input or the only FASTQ file for a single-ended input. Note that the chunker utility would automatically infer if the input file is compressed or not by seeing the `.gz` extension.
2. **fastq2Path** - The path of the second FASTQ file for a pair-ended input. For a single-ended input, leave this field empty (Like this -> `<fastq2Path></fastq2Path>`). You can't ommit it though.
3. **outputFolder** - The output folder path where the chunks would be placed. Prefix with `local:` if you want this folder to be in the local directory.
4. **compLevel** - The chunks made are compressed by the level specified in this field. Value can be from 1 to 9.
5. **chunkSizeMB** - The size of the chunks in MB.
6. **blockSizeMB** - The input FASTQ files are read block by block. In this field, you specify how big such block should be in MBs. The bigger the block size, the more memory would be consumed. However, bigger block size can also mean better performance.
7. **driverMemGB** - The memory used by the chunker utility in GBs.
8. **numThreads** - The number of threads performing chunking. Try to use as many threads as possible as allowed by your system.
9. **interleave** - `true` or `false` depending on whether you want to interleave the contents of the two FASTQ files for a paired-ended input into single chunks. For SparkGA, when using pair-ended input, you must always set this field to `true`. For single-ended input, this field is ignored and can even be ommitted.

### Configuration file for the downloader utility
An example configuration file is placed in the config folder by the name of `downloadExample.xml`.

1. **mode** - Can be either `yarn-cluster` or `yarn-client`.
2. **inputFolder** - The HDFS folder containing all the reference and index files.
3. **refFileName** - The reference FASTA file name. Name of files accompanying that FASTA file, such as \*.dict and *.fasta.fai would be infered automatically by the downloader utility.
4. **snpFileName** - The name of the vcf file containing the known snps. The name of the accompanying index file would be infered automatically by the downloader utility.
5. **indelFileName** - The name of the vcf file containing the known indels. The name of the accompanying index file would be infered automatically by the downloader utility. Leave this field empty if you don't want to use such file with the the indel realigner.
6. **sfFolder** - This is the folder, where all the reference and index files would be stored. This folder would be in a local directory. This folder would be present on each node, and would be created by the downloader utility if not present.
7. **numNodes** - The total number of nodes in the cluster.
8. **execMemGB** - The executor memory in GB. Its value should be between 0.5 and 1 of the total memory in each data node, so that exactly one executor runs and therefore copies files on each node. It is assumed here that each node has the same amount of memory. Moreover, it is assumed that no other program is running on the cluster.
9. **driverMemGB** - The driver memory in GB.

## Running SparkGA
SparkGA consists of three parts. The python script `runAll.py` will run these three parts one after another using the `runPart.py` script. The `runPart.py` script takes two parameters. The first being the input xml file, while the second being the part number. For example, if the xml file's path is config/config.xml, you can run the Part 1 by typing `runPart.py config/config.xml 1`. If you want to run all the parts one after the other without your intervention, you can simply use the `runAll.py` script which takes the input xml file as a parameter. The `runAll.py` script would also save the Spark's output in log files (log1.txt, log2.txt and log3.txt respectively for the three parts) for each part. You can also give runAll.py an optional second parameter to start execution from a certain part. For example, `runAll.py config/config.xml 2` would start execution from the second part, rather than the first part.

If you are not running the chunker utility in parallel (to make chunks on the fly), and want to run the chunker utility separately first before running SparkGA, you can use the `runChunker.py` python script which takes an xml file as an input (See Section **Configuration file for the chunker utility** for more details on that xml file).







