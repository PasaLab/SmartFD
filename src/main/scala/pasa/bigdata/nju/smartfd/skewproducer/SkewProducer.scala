package pasa.bigdata.nju.smartfd.skewproducer

import java.util

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import pasa.bigdata.nju.smartfd.conf.Conf
import pasa.bigdata.nju.smartfd.inductor.InductorInterface
import pasa.bigdata.nju.smartfd.scheduler.Scheduler
import pasa.bigdata.nju.smartfd.scheduler.onetime.OneTimeSchedulerImpl
import pasa.bigdata.nju.smartfd.structures.{FDList, FDSet, FDTree, FDs}
import pasa.bigdata.nju.smartfd.validator.ValidationResult

import scala.collection.mutable.ArrayBuffer

/**
  * a producer for skew attribute
  */
class SkewProducer extends Producer with Serializable {
  def this(sortedDataSet: RDD[Array[Int]],
           inductor: InductorInterface,
           conf: Conf,
           negCover: FDSet,
           currentAttribute: Int,
           posCover: FDTree){
    this
    this.sortedDataSet = sortedDataSet
    this.inductor = inductor
    this.conf = conf
    this.negCover = negCover
    this.currentAttribute = currentAttribute
    this.posCover = posCover
  }

  @transient
  private var sortedDataSet: RDD[Array[Int]] = null
  @transient
  private var inductor: InductorInterface = null
  @transient
  private var negCover: FDSet = null
  @transient
  private var posCover: FDTree = null

  private var conf: Conf = null
  private var currentAttribute: Int = 0

  /**
    * validate candidate fds whose lhs contains skew attributes
    */
  override def validate(): Unit = {
    val scheduler: Scheduler = new OneTimeSchedulerImpl(posCover, conf, currentAttribute)
    var assignments = scheduler.getTasks(new ArrayBuffer[FDs]())._2
    while(assignments.size != 0){
      var currentAssignment = assignments(0)
      while(currentAssignment.size > 0){
        val candidateFD = currentAssignment(0)
        val lhs = candidateFD.getLhs
        val rhss = candidateFD.getRhss
        val fDList = validate(sortedDataSet, lhs, rhss)
        inductor.updatePositiveCover(fDList, currentAttribute)
        currentAssignment = prune(currentAssignment)
      }
      assignments = scheduler.getTasks(new ArrayBuffer[FDs]())._2
    }

  }

  //remove invalid fds from task
  private def prune(assignment: Array[FDs]): Array[FDs] = {
    val arrayBuffer = new ArrayBuffer[FDs]()
    for(i <- 1 until assignment.size){
      val fd = assignment(i)
      val lhs = fd.getLhs
      val rhss = fd.getRhss
      var rhsAttr = rhss.nextSetBit(0)
      while(rhsAttr > 0){
        if(!posCover.containsFdOrGeneralization(lhs, rhsAttr)){
          rhss.clear(rhsAttr)
        }
        rhsAttr = rhss.nextSetBit(rhsAttr + 1)
      }
      if(rhss.cardinality() != 0) arrayBuffer += new FDs(lhs.get(0, conf.numAttributes), rhss.get(0, conf.numAttributes))
    }
    arrayBuffer.toArray
  }

  //get set positions of a bitSet
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

  //add invalid fds to result set
  private def addValidatorResult(invalidFD: util.HashSet[util.BitSet],
                                 fDList: FDList): Unit ={
    val iter = invalidFD.iterator()
    while(iter.hasNext){
      val bitSet = iter.next()
      if(!negCover.contains(bitSet)){
        fDList.add(bitSet)
        negCover.add(bitSet)
      }
    }
  }

  //validate fds
  private def validate(sortedDataSet: RDD[Array[Int]],
                       lhs: util.BitSet,
                       rhss: util.BitSet): FDList = {
    implicit val caseInsensitiveOrdering = new Ordering[LhsKey] {
      override def compare(x: LhsKey, y: LhsKey): Int = {
        val xData = x.data
        val yData = y.data
        var i = 0
        while(i < xData.size){
          if(xData(i) > yData(i)){
            return 1
          } else if(xData(i) < yData(i)){
            return -1
          }
          i += 1
        }
        return 0
      }
    }

    val lhsBitPosition = getBitPosition(lhs)
    val validateResult = sortedDataSet.map{f =>
      val lhsKey = for(attr <- lhsBitPosition) yield f(attr)
      (new LhsKey(lhsKey), f)
    }.repartitionAndSortWithinPartitions(new HashPartitioner(conf.numPartition))
      .mapPartitions{iter =>
        Iterator(validate(iter, lhs, rhss, conf))
      }.collect().map(f=>f.fdSet)
    val fdSet = validateResult(0)
    for(i <- 1 until validateResult.size){
      fdSet.addAll(validateResult(i))
    }

    val fDList: FDList = new FDList(conf.numAttributes)
    addValidatorResult(fdSet, fDList)
    fDList
  }

  //validate fds
  private def validate(iter: Iterator[(LhsKey, Array[Int])],
                       lhs: java.util.BitSet,
                       rhss: java.util.BitSet,
                       conf: Conf): ValidationResult ={
    val rhssBitPosition = getBitPosition(rhss)
    val validationResult = new ValidationResult(0, conf.numAttributes)
    val numAttributes = conf.numAttributes

    val refinedRhs: util.BitSet = rhss.get(0, conf.numAttributes)
    if(iter.hasNext){
      var preLine = iter.next()
      while(iter.hasNext){
        val curLine = iter.next()
        if(preLine._1.equals(curLine._1)){//lhs is equal, need to compare rhss
          var i = 0
          while(i < rhssBitPosition.size){
            val index = rhssBitPosition(i)
            if (preLine._2(index) != curLine._2(index)) {
              refinedRhs.clear(index)
              val equalAttrs: util.BitSet = new util.BitSet(numAttributes)
              matches(equalAttrs, preLine._2, curLine._2)
              validationResult.fdSet.add(equalAttrs)
            }
            i += 1
          }
        }else{// lhs is not equal
          preLine = curLine
        }
      }
    }

    rhss.andNot(refinedRhs)
    var invalidFD: FDs = null
    if(rhss.cardinality() != 0){
      invalidFD = new FDs(lhs.get(0, conf.numAttributes), rhss.get(0, conf.numAttributes))
    }

    validationResult
  }

  //compare tow records
  private def matches(equalAttrs: util.BitSet, t1: Array[Int], t2: Array[Int]): Unit ={
    var i = 0
    while(i < t1.length){
      if(t1(i)==t2(i)){
        equalAttrs.set(i)
      }
      i += 1
    }
  }

}
