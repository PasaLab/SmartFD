# SmartFD

SmartFD is an efficient and scalable algorithm for discovering all minimal, non-trival functional dependencies (FDs) from large-scale distributed datasets. Furthermore, SmartFD is built on the widely-used distributed data-parallel platform Apache Spark.

# News

SmartFD won the first place in [the 4th National University Cloud Computing Application Innovation Competition Big Data Skills Challenge](https://cloud.seu.edu.cn/series) held in 2017-2018, China.

# Prerequisites

- **Apache Spark:** As SmartFD is built on top of Spark, you need to get the Spark installed first. If you are not clear how to setup Spark, please refer to the guidelines [here](http://spark.apache.org/docs/latest/). Currently, SmartFD is developed on the APIs of Spark 2.1.0 version.
- **Apache HDFS:** SmartFD uses HDFS as the distributed file system. If you are not clear how to setup HDFS, please refer to the guidelines [here](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html). The HDFS version is 2.7.3. 
- **Java:** The JDK version is 1.7.  

## Compile
 
SmartFD is built using [sbt](https://www.scala-sbt.org/). We have offered a default **build.sbt** file to manage the whole project. Make sure you have installed sbt and you can just run `sbt assembly` in the root directory to get an assembly jar in the `target/scala-2.10` directory. Note that the default Spark version and Hadoop version are defined in the file **build.sbt**, you can modify it if necessary.

The compiled jar is available at `target/scala-2.10/SparkFD-assembly-1.0.jar`

> Note: A precompiled jar is provided in the `lib` directory.

## Run

The entry of SmartFD is defined in the scala class `pasa.bigdata.nju.smartfd.main.Main`.

The parameters of SmartFD can be configured by appending `--paramKey paramVaule` to the run command.

For instance, run SmartFD with the following command:

	spark-submit --master [SPARK_MASTER_ADDRESS] \
	--executor-memory 20G \
	--driver-memory 20G \
	--executor-cores [Total cores in each executor] \ 
	--conf spark.driver.maxResultSize=20g \
	--conf spark.memory.fraction=0.3 \
	--conf spark.memory.storageFraction=0.5 \
	--conf spark.shuffle.spill.compress=true \
	--class pasa.bigdata.nju.smartfd.main.Main \
	SparkFD-assembly-1.0.jar \
	--inputFilePath ${inputPath} --tempFilePath ${tempPath} --numAttributes ${numAttributes} --numPartition ${numPartitions} --outputFilePath ${inputPath}_results

 The run command contains the following parameters:

- `<inputFilePath>`: The input data path on HDFS.
- `<tempFilePath>`: The temporary data path on HDFS.
- `<numAttributes>`: The number of attributes of the input dataset.
- `<numPartition>`: The computation parallelism on Spark.
- `<outputFilePath>`: The output data path on HDFS.

**Note:** The parameters configured in the run command have higher priority than those in the configuration file. To find more information about the configuration file, please refer to the [Configuration File](https://github.com/PasaLab/SmartFD/blob/master/src/main/scala/pasa/bigdata/nju/smartfd/conf/Conf.scala).


## Demo

The `demo` directory contais 11 datasets (the input file and the output file). 

The format of the input file is `csv`, the default column seperator is comma(`,`). 

All minimal, non-trival functional dependencies are in the output file.

### Output format 

Each FD is represented by the Lhs and Rhs.

For example, `[column1,column5]:column3` indicates the FD `{column1,column5}-> column3`. 

In addition, the FDs that have the same Lhs can be combined.

Given the FDs `[column1,column5]:column3` and `[column1,column5]:column4`, the combined result is `[column1,column5]:column3,column4`.

## Licence

Please contact the authors for the licence info of the source code.
