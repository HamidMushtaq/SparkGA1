# SparkGA

SparkGA is an Apache Spark based scalable implementation of the best practice DNA analysis pipeline by Broad Institute. A paper describing its implementation has been accepted for the ACM-BCB 2017 conference. So, if you use SparkGA for your work, please cite the following paper.

> Hamid Mushtaq, Frank Liu, Carlos Costa, Gang Liu, Peter Hofstee, and Zaid
Al-Ars. 2017. SparkGA: A Spark Framework for Cost Effective, Fast and
Accurate DNA Analysis at Scale. In Proceedings of ACM-BCB ’17, Boston,
MA, USA, August 20-23, 2017.

## System requirements and Installation
For SparkGA, you must have the following software packages installed in your system/cluster. Note that SparkGA runs on a yarn cluster, so when running in cluster mode, the cluster should have been properly setup. Moreover, your system/cluster must be Linux-based.

* Java 1.7 or greater
* Hadoop 2.4 or greater
* Apache Spark 2.0.0 or greater

Note that your Spark version should match the hadoop version. For example, if you have hadoop version 2.5.1, you must use spark-2.x.x-bin-hadoop2.4. Moreover you must have an evironment variable ```$SPARK_HOME``` that points to the Spark directory. When running in cluster mode (yarn-client or yarn-cluster), you must add the following to ```$SPARK_HOME/conf/spark-env.sh```,

> HADOOP_CONF_DIR=$HADOOP_INSTALL/etc/hadoop

This of course assumes that you have an environment variable $HADOOP_INSTALL that points to the hadoop's home directory.

## Files required

Before you run SparkGA, make sure you have the following files available.

1. The input xml file which contains configuration parameters for running SparkGA. Each tag for that xml file is explained in Section **Configuration files**.

2. All the reference and index files. This means files such as the fasta, bwt, dict and known snp vcf files etc. These are the same files that are required for the classical GATK pipeline from broad. These files are required to be available in a folder or in the HDFS, depending upon whether you are running SparkGA in local or cluster (yarn-client or yarn-cluster) mode. For cluster mode, you can put these files on the local directories of each node in three different ways. This is explained later in this document in Section **Putting reference and index files on each node of the cluster**. Note that the .dict file should also be present in SparkGA'S main folder. 

3. The input fastq files, either in compressed (gzipped) or uncompressed form. For a single-ended input, only one fastq file is requied, while for pair-ended input, two files are required. The chunker program can be used to make chunks or interleaved chunks (for paired-ended input) from the input files. However, this chunking can be done in parallel with the execution of SparkGA, by simply adding two extra parameters in the input xml file. More on this is described later in this document. When chunking is done in parallel, SparkGA will start execution as soon as the first chunks are made available by the chunker program. In this way, the chunker program could be uploading the chunks on the fly.

4. All the tools in one directory. This path of this directory is specified as the value of ```toolsFolder``` in the input xml file. The following tools should be in the ```toolsFolder``` directory.
	* bwa 0.7.x
	* bedtools 2.x
	* Picard's Mardkuplicates.jar and CleanSam.jar, version 1.11x
	* GATK's GenomeAnalysisTK.jar, version 3.x

5. In SparkGA'a main folder, you must have the file ```lib/htsjdk-1.143```. Besides that, to run the SparkGA program, you must use the python scripts ```runPart.py``` (to run each part, which are mapping, dynamic load balancing and variant calling, separately) or ```runAll.py``` (to run all the parts one after another). These scripts should be in SparkGA's main folder. Note that the ```runAll.py``` script internally uses ```runPart.py``` to run all the parts one after another. More details are given in the section **Running SparkGA**.

6. In SparkGA's main folder, you must have the main program ```sparkga1_2.11-1.0.jar``` in a folder named ```program```.

7. The chunker program ```chunker_2.11-1.0.jar``` should be in the folder ```chunker``` inside the SparkGA's main folder. If you want to make chunkers separately (not on the fly), you must use the the python script ```runChunker.py``` in SparkGA's main folder. In either case, you also need a separate xml configuration file for the chunker program. Each tag for such xml file is explained in Section **Configuration files**.

8. If you are using the downloader utility (More on the downloader utility later in the text), you must have the downloader program ```filesdownloader_2.11-1.0.jar``` in the folder ```filesDownloader```. To run the downloader utility, you must use the python script ```runDownloader.py``` in SparkGA's main folder. Like SparkGA's main program and the chunker utility, the downloader utility requires an input xml file, which is explained in Section **Configuration files**.

## Putting reference and index files on each node of the cluster
// To be added

## Configuration files

### Configuration file for SparkGA
1. **mode** - The mode of running. Its value could be local, yarn-client or yarn-cluster (we will collectively call them cluster modes from now on).
2. **refPath** - The path of the fasta file. All the accompanying files, such as \*.fasta.bwt, \*.dict and \*.fasta.pac etc. should be in the same folder as the fasta file. The names of those files would be infered from the fasta file. For local mode, these files should be in some local directory, while for cluster modes, they should be in an HDFS directory. Note that if you have already downloaded these files to each node, then only giving the name of the fasta file would suffice. This rule also applies for ```snpPath``` and ```indelPath```.
3. **snpPath** - The path to the snp vcf file used for known sites in base recalibration. 
4. **indelPath** - The path to the indel vcf file used for known indels in indel realignment. If you want the indel realigner to not use any known indels, you can leave this field empty, or even ommit this tag altogehter.
5. **exomePath** - The path of the bed or intervals file for exome sequencing. For local mode, this should be a local path, while for cluster modes and HDFS path. Note that in cluster modes, this file is read directly from the HDFS. Therefore, this file must be present in the HDFS for cluster modes.
6. **inputFolder** - The folder that contains the input chunks. For local mode, this would be a local directory, while for cluster modes, an HDFS directory. If you specify `chunkerConfigFilePath` and `chunkerGroupSize` (See tags at number 38 and 39), the chunker utility would create this folder and upload chunks on the fly.
7. **outputFolder** - The output folder that will contain the final output vcf file namely `sparkCombined.vcf`. Moreover, this folder would also contain the log folder with log files, the purpose of which is to let  user to check the progression of the program. 
8. **toolsFolder** - This folder contains all the tools for executing the GATK pipeline, such as bwa, Markduplicates.jar and GenomeAnalysiTK.jar etc. This folder would always be in a local directory, regardless of the mode. For cluster modes, the python script `runPart.py` would send the programs to each executor, using `--files` when executing `spark-submit`.
9. **tmpFolder** - This folder is the one used for storing temporary files. This folder would be in a local directory. For cluster modes, this folder would be present on each node.
10. **sfFolder** - This is the folder, where all the reference and index files are stored. This folder would be in a local directory. For cluster modes, this folder would be present on each node.
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
31. **numThreads3** - The number of threads to use for variant discovery tools of Part 3.
32. **numTasks3** - The number of tasks per each executor in Part 3. In cluster modes, the total number of tasks running on the whole cluster would then be `numInstances3 * numTasks3`. For local mode, on the other hand, the total number of tasks would simply be equal to `numTasks3`. 
33. **standEC** - The value of `stand_emit_conf` for haplotype caller.
34. **standCC** - The value of `stand_call_conf` for haplotype caller.
35. **doIndelRealignment** - `true `or `false` depending on whether you want to perform indel realignment.
36. **doPrintReads** - `true `or `false` depending on whether you want to perform print reads.
37. **chunkerConfigFilePath** - If you want to perform chunking of the input FASTQ files on the fly, give the path of the config file for the chunker utility.
38. **chunkerGroupSize** - When chunking is done in parallel, the part1 of SparkGA would process chunks in groups. For example, if you give a group size of 512, and the chunker eventually creates 1500 chunks overall, part1 would first process the 512 files and when all those 512 files are processed, will start processing the next 512 chunks. So, it would complete execution in 3 steps. However, if you can accurately predict the number of chunks that would be created by the chunker, you can give a slightly higher estimate. For example, if you give a value of 1700 for our example, part1 would just use one round. The tasks processing chunk IDs greater than 1499 would simply return without doing anything, as no correspodnding chunk would exist for such IDs. Therefore, giving a slightly higher estimate won't impact the execution speed of Part 1 as such.

### Configuration file for the chunker utility
// To be added

### Configuration file for the downloader utility
// To be added

## Running SparkGA
// To be added







