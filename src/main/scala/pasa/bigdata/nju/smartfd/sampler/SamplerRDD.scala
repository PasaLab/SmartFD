package pasa.bigdata.nju.smartfd.sampler

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import pasa.bigdata.nju.smartfd.conf.Conf
import pasa.bigdata.nju.smartfd.validator.Validator


class SamplerRDD(pre: RDD[Validator], conf: Conf) extends RDD[Sampler](pre){
  override def compute(split: Partition, context: TaskContext): Iterator[Sampler] = {
    val partition = firstParent[Validator].iterator(split, context).next().dataSet
    if(conf.useMultiThreadSampler){
      Iterator(new MultiThreadSampler(conf, partition))
    }else{
      Iterator(new SingleThreadSampler(conf, partition))
    }
  }

  override protected def getPartitions: Array[Partition] = firstParent[Array[Int]].partitions
}
