package pasa.bigdata.nju.smartfd.data_process

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Get and clean dataset
  * args(0): input file path
  * args(1): rows
  * args(2): attributes
  * args(3): partition num
  * args(4): output file path
  */

object GetDataSet {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("GetDataSet"))
    val attributes = args(2).split(",").map(f=>f.toInt)
    sc.textFile(args(0)).zipWithIndex().filter(f=>f._2 < args(1).toLong).map(f=>f._1.split(",")).map{f=>
      val result = for(i <- attributes) yield {
        f(i)
      }
      for(i<-0 until result.size){
        val res = for(c <- result(i) if !c.equals('"')) yield {
          c
        }
        if(res != ""){
          result(i) = res
        }else{
          result(i) = "?"
        }
      }
      result.mkString(",")
    }.repartition(args(3).toInt)
      .saveAsTextFile(args(4))
  }
}
