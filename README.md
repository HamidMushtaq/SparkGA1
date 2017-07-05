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
// To be added

### Configuration file for SparkGA
// To be added

### Configuration file for the chunker utility
// To be added

### Configuration file for the downloader utility
// To be added

## Running SparkGA
// To be added







