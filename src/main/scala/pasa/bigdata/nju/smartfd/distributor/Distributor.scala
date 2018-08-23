package pasa.bigdata.nju.smartfd.distributor

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pasa.bigdata.nju.smartfd.conf.Conf

import scala.collection.mutable

/**
  * Data pre-processor, including attribute sorting and record encoding
  */
class Distributor() extends Serializable {
  var conf: Conf = null
  var dataSet: RDD[Array[String]] = null
  var attributeCardinality: Array[Int] = null//cardinality of attribute(unsorted)
  var attributeValueNumber: Array[mutable.HashMap[String, Int]] = null
  var sortedAttributes: Array[Int] = null//sorted attribute
  var sortedDataSet: RDD[Array[Int]] = null
  var sortedCardinality: Array[Int] = null//cardinality of attribute(sorted)
  var currentAttribute = -1
  var sizeOfDataSet = 0
  var skewAttr: mutable.HashSet[Int] = null
  @transient
  var sc: SparkContext = null

  def this(conf: Conf, dataSet: RDD[String]){
    this
    this.skewAttr = new mutable.HashSet[Int]()
    this.conf = conf
    this.sc = sc
    this.dataSet = dataSet.map(f=>f.split(conf.inputFileSeparator)).cache()
    dataSet.unpersist()
    this.getAttributeDistribution(this.dataSet, conf.numAttributes)

    if(conf.useRearrangement){
      conf.rearrangeParam match{
        case "variance" => throw new RuntimeException("Have not support yet!" + conf.rearrangeParam)//TODO: add variance
        case "frequent" => sortByFrequent
        case "var_fre" => throw new RuntimeException("Have not support yet!" + conf.rearrangeParam) //TODO: add var_fre
        case "fre_var" =>  sortByFrequentVariance
        case "fre_var_skew" =>  sortByFrequentVarianceSkew
        case _ => throw new RuntimeException("No such rearrangeParam: " + conf.rearrangeParam)
      }
      val broadcast = attributeValueNumber.map(f=>f.map(x=>x._1).zipWithIndex.toMap)
      this.sortedDataSet = this.dataSet.mapPartitions{iter=>iter.map(f => for(i <- sortedAttributes) yield broadcast(i)(f(i)))}.cache()
      this.dataSet.unpersist(false)
      this.attributeValueNumber = attributeValueNumber
        .zip(attributeCardinality)
        .sortBy(_._2).reverse.map(_._1)
    }else{
      this.sortedAttributes =  (0 until conf.numAttributes).toArray
      this.sortedCardinality = attributeCardinality
      val broadcast = attributeValueNumber.map(f => f.map(x=>x._1).zipWithIndex.toMap)
      this.sortedDataSet = codingData(broadcast, this.dataSet, conf.numAttributes).cache()
      this.attributeValueNumber = attributeValueNumber
    }
  }

  /**
    * Sort attribute by frequent and variance
    */
  private def sortByFrequentVariance: Unit ={
    val variances = new Array[Double](conf.numAttributes)
    for(i <- 0 until attributeValueNumber.size){
      variances(i) = attributeValueNumber(i).values.toArray
        .map(f => math.pow(f, 2.0)).sum / attributeValueNumber(i).size.toDouble
    }
    val attributeNumber: Array[Int] = (0 until conf.numAttributes).toArray
    val sortAttributeByCardinality = attributeCardinality
      .zip(attributeNumber)
      .sortBy(_._1)
      .reverse

    val largeKeyAttribute = sortAttributeByCardinality
      .filter(f => f._1 > conf.numPartition)//TODO: choose better boundary instead of conf.numPartition
      .map(f=>(f._2, variances(f._2)))
      .sortBy(_._2)
      .map(_._1)

    val smallKeyAttribute = sortAttributeByCardinality
      .filter(f => f._1 <= conf.numPartition)
      .map(_._2)

    this.sortedAttributes = largeKeyAttribute.++(smallKeyAttribute)
    this.sortedCardinality = sortedAttributes.map(f=>attributeCardinality(f))
  }

  /**
    * Sort attributes by cardinality,variance and skewness
    */
  private def sortByFrequentVarianceSkew: Unit = {
    val skewness = attributeValueNumber.map(f=>f.map(x=>x._2.toDouble / sizeOfDataSet).max).zipWithIndex.filter(f=>f._1 > conf.skewThreshold).sortBy(f=>f._1)
    val skewAttributes = skewness.map(f=>f._2)
    val variances = new Array[Double](conf.numAttributes)
    for(i <- 0 until attributeValueNumber.size){
      variances(i) = attributeValueNumber(i).values.toArray
        .map(f => math.pow(f, 2.0)).sum / attributeValueNumber(i).size.toDouble
    }
    val attributeNumber: Array[Int] = (0 until conf.numAttributes).toArray
    val sortAttributeByCardinality = attributeCardinality
      .zip(attributeNumber)
      .sortBy(_._1)
      .reverse

    val largeKeyAttribute = sortAttributeByCardinality
      .filter(f => f._1 > conf.numPartition)//TODO: choose better boundary instead of conf.numPartition
      .map(f=>(f._2, variances(f._2)))
      .sortBy(_._2)
      .map(_._1)

    val smallKeyAttribute = sortAttributeByCardinality
      .filter(f => f._1 <= conf.numPartition)
      .map(_._2)
    val nonSkewAttributes = largeKeyAttribute.++(smallKeyAttribute).filter(f => !skewAttributes.contains(f))
    this.sortedAttributes = nonSkewAttributes ++ skewAttributes
    this.sortedCardinality = sortedAttributes.map(f=>attributeCardinality(f))
    val numOfSkewAttribute = skewness.filter(f=>f._1 * sizeOfDataSet > conf.skewDataSize).size
    for(i <- (conf.numAttributes - numOfSkewAttribute) until conf.numAttributes){
      skewAttr += i
    }
  }

  def isSkew(attribute: Int): Boolean ={
    return skewAttr.contains(attribute)
  }

  private def sortByFrequent: Unit ={
    val attributeNumber: Array[Int] = (0 until conf.numAttributes).toArray
    val sortAttributeByCardinality = attributeCardinality
      .zip(attributeNumber)
      .sortBy(_._1).reverse
    this.sortedAttributes = sortAttributeByCardinality.map(_._2)
    this.sortedCardinality = sortAttributeByCardinality.map(_._1)
  }


  /**
    * calculating the cardinality of each attribute of the input file
    *
    * @return the cardinality of each attribute of the input file
    */
  def getAttributeDistribution(dataSet:RDD[Array[String]], numAttributes: Int): Unit = {

    val cardinalityOfAttribute = dataSet.mapPartitions(iter => {
      val attributes = new Array[mutable.HashMap[String, Int]](numAttributes)
      for(i <- 0 until numAttributes){
        attributes(i) = new mutable.HashMap[String, Int]()
      }
      while(iter.hasNext){
        val next = iter.next()
        if(next.size != numAttributes) throw new RuntimeException("wrong data: " + next.mkString(","))
        var i = 0
        while(i < numAttributes){
          if(attributes(i).contains(next(i))){
            attributes(i) += (next(i) -> (attributes(i)(next(i)) + 1))
          }else{
            attributes(i) += (next(i) -> 1)
          }
          i+=1
        }
      }
      Iterator(attributes)
    }).reduce((x: Array[mutable.HashMap[String, Int]],
                            y: Array[mutable.HashMap[String, Int]]) => combine(x, y, numAttributes))
    this.attributeValueNumber = cardinalityOfAttribute
    this.attributeCardinality = cardinalityOfAttribute.map(f=>f.size)
    this.sizeOfDataSet = cardinalityOfAttribute(0).map(f=>f._2).sum
  }

  /**
    * combine the info of each partition
    * @param x static info
    * @param y static info
    * @param numAttributes number of attributes
    * @return
    */
  private def combine(x: Array[mutable.HashMap[String, Int]],
                      y: Array[mutable.HashMap[String, Int]],
                      numAttributes: Int) = {
    for(i<- 0 until numAttributes){
      for((k, v) <- y(i)){
        if(x(i).contains(k)){
          x(i) += (k -> (x(i)(k) + v))
        }else{
          x(i) += (k -> + v)
        }
      }
    }
    x
  }

  /**
    * Convert data into digital coding
    * @param keyMap transformation rule
    * @param dataSet data set
    * @param numAttributes number of attributes
    * @return
    */
  private def codingData(keyMap: Array[Map[String, Int]],
                         dataSet: RDD[Array[String]],
                         numAttributes: Int): RDD[Array[Int]] = {
    dataSet.map{f =>
    for(i <- 0 until numAttributes) yield keyMap(i)(f(i))
    }.map(f=>f.toArray)
  }

}
