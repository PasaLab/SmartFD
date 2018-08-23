package pasa.bigdata.nju.smartfd.validator

import java.util

import pasa.bigdata.nju.smartfd.conf.Conf
import pasa.bigdata.nju.smartfd.structures.FDs

import scala.collection.mutable

class SingleThreadValidator extends AbstractValidator with Serializable {
  def this(dataSet: Array[Array[Int]],
           partitionID: Int,
           cardinalityOfAttribute: Array[Int],
           currentAttribute: Int,
           conf: Conf){
    this
    this.partitionID = partitionID
    this.cardinalityOfAttributes = cardinalityOfAttribute
    this.dataSet = dataSet
    this.currentAttribute = currentAttribute
    this.conf = conf
  }

  override var partitionID: Int = 0
  override var cardinalityOfAttributes: Array[Int] = null
  override var dataSet:Array[Array[Int]] = null
  override var currentAttribute: Int = 0
  override var conf: Conf = null
  var validateTimes = 0
  val validatedFD = new mutable.HashMap[java.util.BitSet, FDs]()

  /**
    * validate fds
    * @param fds candidate fds
    * @return validate result
    */
  override def validate(fds: Array[FDs]) = {
    validateTimes += 1
    val result: ValidationResult = new ValidationResult(partitionID, conf.numAttributes)
    if(dataSet == null || dataSet.size == 0){
      for(fd <- fds) {
        val rhss: util.BitSet = fd.getRhss
        if (rhss.cardinality() != 0) {
          result.validations = result.validations + rhss.cardinality()
        }
      }
      result.validatorTime = 0l
      result
    } else{
      val validatorStart = System.currentTimeMillis()
      for(fd <- fds){
        val lhs: util.BitSet = fd.getLhs
        val rhss: util.BitSet = fd.getRhss
        if(rhss.cardinality() != 0){
          result.validations = result.validations + rhss.cardinality()
          if (lhs.cardinality() == 0) {
            var rhsAttr: Int = rhss.nextSetBit(0)
            val invalidRhs: util.BitSet = new util.BitSet(conf.numAttributes)
            while (rhsAttr >= 0) {
              if (cardinalityOfAttributes(rhsAttr) > 1) {
                invalidRhs.set(rhsAttr)
                result.addInvalidFDs(lhs.get(0, conf.numAttributes), invalidRhs.get(0, conf.numAttributes))
                invalidRhs.clear(rhsAttr)
              }
              result.intersections += 1
              rhsAttr = rhss.nextSetBit(rhsAttr + 1)
            }
          }else if (lhs.cardinality() == 1) {
            val lhsAttribute: Int = lhs.nextSetBit(0)
            var rhsAttr: Int = rhss.nextSetBit(0)
            while (rhsAttr >= 0) {
              if (!checkSingleLhsAttribute(lhsAttribute, rhsAttr, dataSet, conf.numAttributes, result)) {
                val invalidRhs: util.BitSet = new util.BitSet(conf.numAttributes)
                invalidRhs.set(rhsAttr)
                result.addInvalidFDs(lhs.get(0, conf.numAttributes), invalidRhs.get(0, conf.numAttributes))
                invalidRhs.clear(rhsAttr)
              }
              result.intersections += 1
              rhsAttr = rhss.nextSetBit(rhsAttr + 1)
            }
          } else {
            lhs.clear(currentAttribute)
            val validRhs: util.BitSet = checkMultiLhsAttributes(dataSet, lhs, rhss, conf.numAttributes, currentAttribute, result)
            lhs.set(currentAttribute)
            result.intersections += 1
            rhss.andNot(validRhs)
            if(rhss.cardinality() != 0){
              result.addInvalidFDs(lhs.get(0, conf.numAttributes), rhss.get(0, conf.numAttributes))
            }
          }
        }
      }
      result.validatorTime = System.currentTimeMillis() - validatorStart
      result
    }
  }

  /**
    * validate fds whose lhs has single attribute
    * @param lhsAttribute
    * @param rhsAttribute
    * @param dataSet
    * @param numAttributes
    * @param result
    * @return
    */
  private def checkSingleLhsAttribute(lhsAttribute: Int,
                                      rhsAttribute: Int,
                                      dataSet: Array[Array[Int]],
                                      numAttributes: Int,
                                      result:ValidationResult): Boolean = {
    if(dataSet == null || dataSet.size == 0){
      //logInfo("check single: " + (System.currentTimeMillis() - time) + "ms")
      return true
    }else{
      var pre = 0
      var i = 0
      while(i < dataSet.size){
        if(dataSet(i)(lhsAttribute)!=(dataSet(pre)(lhsAttribute))){
          pre = i
        }else{
          if(dataSet(pre)(rhsAttribute)!=(dataSet(i)(rhsAttribute))){
            val equalAttrs: util.BitSet = new util.BitSet(numAttributes)
            matches(equalAttrs, dataSet(pre), dataSet(i))
            result.fdSet.add(equalAttrs)
            //logInfo("check single: " + (System.currentTimeMillis() - time) + "ms")
            return false
          }
        }
        i += 1
      }
      return true
    }
  }

  /**
    * validate fds whose lhs has multiple attributes
    * @param dataSet
    * @param lhs
    * @param rhss
    * @param numAttributes
    * @param currentAttribute
    * @param result
    * @return
    */
  private def checkMultiLhsAttributes(dataSet: Array[Array[Int]],
                                      lhs: util.BitSet,
                                      rhss: util.BitSet,
                                      numAttributes: Int,
                                      currentAttribute: Int,
                                      result:ValidationResult): util.BitSet = {

    val lhsBitPosition = getBitPosition(lhs)
    val refinedRhs: util.BitSet = rhss.get(0, numAttributes)
    var posi = 0
    val subClusters = new java.util.HashMap[ClusterIdentifier, Array[Int]](1)
    while(posi < dataSet.size) {
      val clusterID = dataSet(posi)(currentAttribute)
      val clusterIdentifier = new ClusterIdentifier(for(index <- lhsBitPosition) yield dataSet(posi)(index))
      subClusters.put(clusterIdentifier, dataSet(posi))
      posi += 1
      while(posi < dataSet.size && dataSet(posi)(currentAttribute)==(clusterID)){
        val curClusterIdentifier = new ClusterIdentifier(for(index <- lhsBitPosition) yield dataSet(posi)(index))

        if(subClusters.containsKey(curClusterIdentifier)){
          val rhsClusters = subClusters.get(curClusterIdentifier)
          var rhsAttr: Int = refinedRhs.nextSetBit(0)
          while (rhsAttr >= 0) {
            if (dataSet(posi)(rhsAttr)!=(rhsClusters(rhsAttr))) {
              refinedRhs.clear(rhsAttr)
              val equalAttrs: util.BitSet = new util.BitSet(numAttributes)
              matches(equalAttrs, rhsClusters, dataSet(posi))
              result.fdSet.add(equalAttrs)
              if (refinedRhs.isEmpty) return refinedRhs
            }
            rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)
          }
        }else{
          subClusters.put(curClusterIdentifier, dataSet(posi))
        }
        posi += 1
      }
      subClusters.clear()
    }
    return refinedRhs
  }


  /**
    * get the index of each set bit
    * @param bitSet
    * @return
    */
  private def getBitPosition(bitSet: util.BitSet): Array[Int] = {
    val bitPosition = new Array[Int](bitSet.cardinality())
    var attr: Int = bitSet.nextSetBit(0)
    var index = 0
    while (attr >= 0) {
      bitPosition(index) = attr
      attr = bitSet.nextSetBit(attr + 1)
      index += 1
    }
    bitPosition
  }

  /**
    * validate data structure
    * @param cluster
    */
  class ClusterIdentifier(var cluster: Array[Int]) {

    def set(index: Int, clusterId: Int) {
      this.cluster(index) = clusterId
    }

    def get(index: Int): Int = {
      return this.cluster(index)
    }

    def getCluster: Array[Int] = {
      return this.cluster
    }

    def size: Int = {
      return this.cluster.length
    }

    override def hashCode: Int = {
      var hash: Int = 1
      var index: Int = this.size
      while (index != 0){
        hash = 31 * hash + this.get(index - 1)
        index -= 1
      }
      return hash
    }

    override def equals(any: Any): Boolean = {//does not be called
      if (!(any.isInstanceOf[ClusterIdentifier])) return false
      val other: ClusterIdentifier = any.asInstanceOf[ClusterIdentifier]
      var index: Int = this.size
      if (index != other.size) return false
      val cluster1: Array[Int] = this.getCluster
      val cluster2: Array[Int] = other.getCluster
      while (index != 0){
        if ((cluster1(index -1)^(cluster2(index - 1))) != 0) return false
        index -= 1
      }
      return true
    }

    override def toString: String = {
      val builder: StringBuilder = new StringBuilder
      builder.append("[")
      for(i <- 0 until this.size){
        builder.append(this.cluster(i))
        if (i + 1 < this.size) builder.append(", ")
      }
      builder.append("]")
      return builder.toString
    }
  }

  private def matches(equalAttrs: util.BitSet, t1: Array[Int], t2: Array[Int]): Unit ={
    var i = 0
    while(i < t1.length){
      if(t1(i)==t2(i)){
        equalAttrs.set(i)
      }
      i += 1
    }
  }

  def calculateHash(line: Array[Int], lhsAttributes: IndexedSeq[Int]): Int = {
    var hash: Int = 1
    for(lhs <- lhsAttributes){
      hash = 31 * hash + line(lhs)
    }
    return hash
  }

  def isLhsEqual(line1: Array[Int], line2: Array[Int], lhsAttributes: IndexedSeq[Int]): Boolean = {
    for(lhs <- lhsAttributes){
      if(line1(lhs) != line2(lhs)){
        return false
      }
    }
    return true
  }

}
