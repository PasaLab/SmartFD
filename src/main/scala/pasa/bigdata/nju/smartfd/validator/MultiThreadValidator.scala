package pasa.bigdata.nju.smartfd.validator

import java.util
import java.util.concurrent
import java.util.concurrent.{Callable, ExecutionException, Future}

import pasa.bigdata.nju.smartfd.conf.Conf
import pasa.bigdata.nju.smartfd.structures.FDs

import scala.collection.mutable


class MultiThreadValidator extends AbstractValidator with Serializable {
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
  val validatedFD = new mutable.HashMap[java.util.BitSet, FDs]()

  /**
    * validate fds
    * @param fds candidate fds
    * @return
    */
  override def validate(fds: Array[FDs]) = {
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
      if(fds.size > 0){
        val bitSet: util.BitSet = fds(0).getLhs
        if(bitSet.cardinality() == 0){
          var fdIndex = 0
          while(fdIndex < fds.length){
            val fd = fds(fdIndex)
            val lhs: util.BitSet = fd.getLhs
            val rhss: util.BitSet = fd.getRhss
            if(rhss.cardinality() != 0){
              result.validations = result.validations + rhss.cardinality()
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
            }
            fdIndex += 1
          }
        }else if(bitSet.cardinality() == 1){
          var fdIndex = 0
          while(fdIndex < fds.length){
            val fd = fds(fdIndex)
            val lhs: util.BitSet = fd.getLhs
            val rhss: util.BitSet = fd.getRhss
            if(rhss.cardinality() != 0){
              result.validations = result.validations + rhss.cardinality()
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
            }
            fdIndex += 1
          }
        }else{
          val executors = concurrent.Executors.newFixedThreadPool(conf.numOfThreadUsedInValidator)
          val  futures: util.ArrayList[Future[ValidatorResult]] = new util.ArrayList[Future[ValidatorResult]]()

          var fdIndex = 0
          while(fdIndex < fds.length){
            val fd = fds(fdIndex)
            result.intersections += 1
            val lhs = fd.getLhs
            val rhss = fd.getRhss
            val task = new ValidationTask(dataSet, lhs, rhss, conf.numAttributes, currentAttribute)
            futures.add(executors.submit(task))
            fdIndex += 1
          }
          var futureIndex = 0
          while(futureIndex < futures.size()){
            val future = futures.get(futureIndex)
            try{
              val res = future.get()
              if(res.invalidFD != null){
                result.addInvalidFDs(res.invalidFD.getLhs, res.invalidFD.getRhss)
              }
              result.fdSet.addAll(res.conflictData)
            }catch {
              case ex: ExecutionException => ex.printStackTrace()
              case eI: InterruptedException => {executors.shutdown(); eI.printStackTrace()}
            }
            futureIndex += 1
          }

          executors.shutdown()
          while(!executors.isTerminated){}
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
    * @return
    */
  private def checkMultiLhsAttributes(dataSet: Array[Array[Int]],
                                      lhs: util.BitSet,
                                      rhss: util.BitSet,
                                      numAttributes: Int,
                                      currentAttribute: Int): ValidatorResult = {
    val fdSet: util.HashSet[util.BitSet] = new util.HashSet[util.BitSet]()
    lhs.clear(currentAttribute)
    val lhsBitPosition = getBitPosition(lhs)
    val rhssBitPosition = getBitPosition(rhss)
    val refinedRhs: util.BitSet = rhss.get(0, numAttributes)
    var posi = 0
    while(posi < dataSet.size) {
      val subClusters = new java.util.HashMap[AttributesHash, Array[Int]]()
      val clusterID = dataSet(posi)(currentAttribute)
      val attributesHash = new AttributesHash(dataSet(posi), lhsBitPosition)
      subClusters.put(attributesHash, dataSet(posi))
      posi += 1
      while(posi < dataSet.size && dataSet(posi)(currentAttribute) == (clusterID)){
        val curAttributesHash = new AttributesHash(dataSet(posi), lhsBitPosition)
        val compareData = subClusters.get(curAttributesHash)
        if(compareData != null){
          var rhsIndex = 0
          while(rhsIndex < rhssBitPosition.size){
            val index = rhssBitPosition(rhsIndex)
            if (dataSet(posi)(index)!=compareData(index)) {
              refinedRhs.clear(index)
              val equalAttrs: util.BitSet = new util.BitSet(numAttributes)
              matches(equalAttrs, compareData, dataSet(posi))
              fdSet.add(equalAttrs)
            }
            rhsIndex += 1
          }
        }else{
          subClusters.put(curAttributesHash, dataSet(posi))
        }
        posi += 1
      }
    }
    lhs.set(currentAttribute)
    rhss.andNot(refinedRhs)
    var invalidFD: FDs = null
    if(rhss.cardinality() != 0){
      invalidFD = new FDs(lhs.get(0, conf.numAttributes), rhss.get(0, conf.numAttributes))
    }
    return new ValidatorResult(invalidFD, fdSet)
  }

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

  private def matches(equalAttrs: util.BitSet, t1: Array[Int], t2: Array[Int]): Unit ={
    var i = 0
    while(i < t1.length){
      if(t1(i)==t2(i)){
        equalAttrs.set(i)
      }
      i += 1
    }
  }

  /**
    * validate task for candidate fds those has the same lhs
    * @param dataSet
    * @param lhs
    * @param rhss
    * @param numAttributes
    * @param currentAttribute
    */
  class ValidationTask(dataSet: Array[Array[Int]],
                       lhs: util.BitSet,
                       rhss: util.BitSet,
                       numAttributes: Int,
                       currentAttribute: Int) extends Callable[ValidatorResult]{
    override def call(): ValidatorResult = {
      checkMultiLhsAttributes(dataSet, lhs, rhss, numAttributes, currentAttribute)
    }
  }

  case class ValidatorResult(val invalidFD: FDs, conflictData: util.HashSet[util.BitSet])

  /**
    * a data structure that represent each records for validate fds
    * @param data
    * @param attributes
    */
  class AttributesHash(val data: Array[Int], val attributes: Array[Int]){

    override def hashCode(): Int = {
      var hash: Int = 1
      var index = attributes.length - 1
      while(index >= 0){
        hash = 31 * hash + data(attributes(index))
        index -= 1
      }
      hash
    }

    override def equals(obj: scala.Any): Boolean = {
      if(!obj.isInstanceOf[AttributesHash]){
        false
      }else{
        val other = obj.asInstanceOf[AttributesHash]
        var index = 0
        while(index < attributes.length){
          if(data(attributes(index)) != other.data(attributes(index))){
            return false
          }
          index += 1
        }
        return true
      }
    }
  }
}
