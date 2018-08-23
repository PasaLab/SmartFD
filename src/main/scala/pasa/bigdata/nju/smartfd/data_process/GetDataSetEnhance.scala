package pasa.bigdata.nju.smartfd.data_process

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Get and clean dataset
  * args(0): input file path
  * args(1): attributes
  * args(2): partition num
  * args(3): output file path
  */
object GetDataSetEnhance {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("GetDataSet"))
    val attributes = args(1).split(",").map(f=>f.toInt)
    val file = sc.textFile(args(0)).zipWithIndex().cache()
    var i = 10000000
    while(i <= 160000000){
      file.filter(f=>f._2 < i).map(f=>f._1.split(",")).map{f=>
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
      }.repartition(args(2).toInt)
        .saveAsTextFile(args(3) + s"bots_p_c${attributes.size}_r${i}")
      i = i*2
    }
    file.unpersist()
  }
}
