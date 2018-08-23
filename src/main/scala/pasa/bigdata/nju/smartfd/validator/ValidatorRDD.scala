package pasa.bigdata.nju.smartfd.validator

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import pasa.bigdata.nju.smartfd.conf.Conf


class ValidatorRDD (prev: RDD[Array[Int]],
                    conf: Conf,
                    cardinalityOfAttribute: Array[Int],
                    currentAttribute: Int)
  extends RDD[Validator](prev){
  override def compute(split: Partition, context: TaskContext): Iterator[Validator] = {
    val partition = firstParent[Array[Int]].iterator(split, context)
    if(conf.useMultiThreadValidator){
      Iterator(new MultiThreadValidator(partition.toArray, split.index, cardinalityOfAttribute, currentAttribute, conf))
    }else{
      Iterator(new SingleThreadValidator(partition.toArray, split.index, cardinalityOfAttribute, currentAttribute, conf))
    }
  }

  override protected def getPartitions: Array[Partition] = firstParent[Array[Int]].partitions
}
