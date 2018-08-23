package pasa.bigdata.nju.smartfd.reader

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pasa.bigdata.nju.smartfd.conf.Conf

/**
  * reading the input data set from HBase or HDFS
  * @param inputFromHBase the input data set is resident in HBase or not
  * @param inputFilePath the file path of the input data set
  * @param numPartition number of spark partitions
  * @param sc SparkContext
  */
class Reader(val inputFromHBase: Boolean,
             val inputFilePath: String,
             val numPartition: Int,
             val sc: SparkContext,
             val conf: Conf = new Conf) {
  /**
    * Load the data set.
    * @return the data set to be calculated in form of RDD[String]
    */
  def getDataSet():RDD[String] = {//TODO: if the file has a header?

    if(inputFromHBase){//TODO: load input data set from HBase
      assert(false,"Cannot load input data set from HBase yet")
      val dataSet = sc.textFile(inputFilePath, numPartition)//just for testing(loading input data set from hdfs)
      dataSet
    }else{// load input data set from hdfs
      sc.textFile(inputFilePath, numPartition)
    }

  }

}
