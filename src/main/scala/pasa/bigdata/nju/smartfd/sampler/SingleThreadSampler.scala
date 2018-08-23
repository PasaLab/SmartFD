package pasa.bigdata.nju.smartfd.sampler

import java.util
import java.util.PriorityQueue

import pasa.bigdata.nju.smartfd.conf.Conf
import pasa.bigdata.nju.smartfd.structures.{FDList, FDSet, PositionListIndex}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

class SingleThreadSampler extends AbstractSampler with Serializable {
  def this(conf: Conf, dataSet: Iterator[Array[Int]]){
    this
    this.efficiencyThreshold = conf.efficiencyThreshold
    this.negCover = new FDSet(conf.numAttributes)
    this.numAttributes = conf.numAttributes
    getDataContainer(dataSet, conf.numAttributes, conf.inputFileSeparator, conf.nullEqualsNull)
  }

  def this(conf: Conf, dataSet: Array[Array[Int]]){
    this
    this.efficiencyThreshold = conf.efficiencyThreshold
    this.negCover = new FDSet(conf.numAttributes)
    this.numAttributes = conf.numAttributes
    getDataContainer(dataSet, conf.numAttributes, conf.inputFileSeparator, conf.nullEqualsNull)
  }

  private var attributeRepresentants: util.ArrayList[AttributeRepresentant] = null
  private var queue: PriorityQueue[AttributeRepresentant] = null
  private var compressedRecords: Array[Array[Int]] = null
  private var plis: Array[PositionListIndex]= null
  private var efficiencyThreshold: Float = 0.0f
  private var negCover: FDSet =  null
  private var numAttributes: Int = 0


  /**
    * get the sample data for each partition
    * @return invalid fds(sample result)
    */
  override def takeSample(): FDList = {
    val newNonFds: FDList = new FDList(numAttributes)
    if(this.plis != null && this.compressedRecords != null
      && this.compressedRecords.size > 0 && this.compressedRecords(0).size > 0){

      if (this.attributeRepresentants == null) { // if this is the first call of this method
        val comparator: ClusterOrdering = new ClusterOrdering(this.compressedRecords, this.compressedRecords(0).length - 1, 1)
        for(i <- 0 until plis.size){
          val clusters = plis(i).getClusters()
          for(j <- 0 until clusters.size){
            clusters(j) = clusters(j).sorted(comparator)
          }
          comparator.incrementActiveKey
        }

        this.attributeRepresentants = new util.ArrayList[AttributeRepresentant](numAttributes)
        this.queue = new PriorityQueue[AttributeRepresentant](numAttributes)
        for(i <- 0 until numAttributes){
          val attributeRepresentant: AttributeRepresentant = new AttributeRepresentant(plis(i).getClusters, negCover, numAttributes, i)
          attributeRepresentant.runNext(newNonFds, this.compressedRecords)
          this.attributeRepresentants.add(attributeRepresentant)
          if (attributeRepresentant.getEfficiency > 0.0f) {
            this.queue.add(attributeRepresentant) // If the efficiency is 0, the algorithm will never schedule a next run for the attribute regardless how low we set the efficiency threshold
          }
        }

        if (!(this.queue.isEmpty)) {
          this.efficiencyThreshold = Math.min(0.01f, this.queue.peek.getEfficiency * 0.5f) // This is an optimization that we added after writing the HyFD paper
        }
      } else { // Decrease the efficiency threshold
        if (!(this.queue.isEmpty)) {
          this.efficiencyThreshold = Math.min(this.efficiencyThreshold / 2, this.queue.peek.getEfficiency * 0.9f)
        }
      }

      while (!(this.queue.isEmpty) && (this.queue.peek.getEfficiency >= this.efficiencyThreshold)) {
        val attributeRepresentant: AttributeRepresentant = this.queue.poll()
        attributeRepresentant.runNext(newNonFds, this.compressedRecords)
        if (attributeRepresentant.getEfficiency > 0.0f) {
          this.queue.add(attributeRepresentant)
        }
      }
    }
    newNonFds
  }

  /**
    * used to sort cluster
    * @param sortKeys
    * @param activeKey1
    * @param activeKey2
    */
  private class ClusterOrdering(sortKeys: Array[Array[Int]],
                                var activeKey1: Int,
                                var activeKey2: Int) extends Ordering[Int] {
    def compare(o1: Int, o2: Int): Int = {
      var value1 = this.sortKeys(o1)(this.activeKey1)
      var value2 = this.sortKeys(o2)(this.activeKey1)
      var result = value2 - value1
      if(result == 0){
        value1 = this.sortKeys(o1)(this.activeKey2)
        value2 = this.sortKeys(o2)(this.activeKey2)
        result = value2 - value1
      }
      result
    }
    def incrementActiveKey: Unit ={
      this.activeKey1 = this.increment(this.activeKey1)
      this.activeKey2 = this.increment(this.activeKey2)
    }
    private def increment(number: Int): Int = {
      if(number == this.sortKeys(0).length - 1) {
        0
      }else {
        number + 1
      }
    }
  }

  /**
    * sample data structure
    */
  private class AttributeRepresentant(val clusters: Array[Array[Int]],
                                      var negCover: FDSet,
                                      var numAttributes: Int,
                                      var attributeIndex: Int)
    extends Comparable[AttributeRepresentant] {
    private var windowDistance: Int = 0
    private val numNewNonFds: util.ArrayList[Int] = new util.ArrayList[Int]()
    private val numComparisons: util.ArrayList[Int] = new util.ArrayList[Int]()

    def getEfficiency: Float = {
      val index: Int = this.numNewNonFds.size - 1
      val sumNewNonFds: Int = this.numNewNonFds.get(index)
      val sumComparisons: Int = this.numComparisons.get(index)
      if (sumComparisons == 0) {
        return 0.0f
      }
      return sumNewNonFds.toFloat / sumComparisons
    }

    override def compareTo(o: AttributeRepresentant): Int = { //			return o.getNumNewNonFds() - this.getNumNewNonFds();
      return Math.signum(o.getEfficiency - this.getEfficiency).toInt
    }

    /**
      * sliding window sampling
      */
    def runNext(newNonFds: FDList, compressedRecords: Array[Array[Int]]): Unit = {
      this.windowDistance += 1
      var numNewNonFds: Int = 0
      var numComparisons: Int = 0
      val equalAttrs: java.util.BitSet = new util.BitSet(numAttributes)
      val previousNegCoverSize: Int = newNonFds.size

      for(cluster <- clusters) {
        if (cluster.size > this.windowDistance) {
          var recordIndex: Int = 0
          while (recordIndex < (cluster.size - this.windowDistance)) {
            val recordId: Int = cluster(recordIndex)
            val partnerRecordId: Int = cluster(recordIndex + this.windowDistance)
            matches(equalAttrs, compressedRecords(recordId), compressedRecords(partnerRecordId))
            if (!(this.negCover.contains(equalAttrs))) {
              val equalAttrsCopy: util.BitSet = equalAttrs.get(0, numAttributes)
              this.negCover.add(equalAttrsCopy)
              newNonFds.add(equalAttrsCopy)
            }
            numComparisons += 1
            recordIndex += 1
          }
        }
      }
      numNewNonFds = newNonFds.size - previousNegCoverSize
      this.numNewNonFds.add(numNewNonFds)
      this.numComparisons.add(numComparisons)
    }
  }

  private def matches(equalAttrs: java.util.BitSet, t1: Array[Int], t2: Array[Int]): Unit = {
    equalAttrs.clear(0, t1.length)
    for(i <- 0 until t1.length){
      if ((t1(i) >= 0) && (t2(i) >= 0) && (t1(i) == t2(i))) {
        equalAttrs.set(i)
      }
    }
  }

  //calculate data structure
  def getDataContainer(iter: Iterator[Array[Int]],
                       numAttributes:Int,
                       inputFileSeparator: String,
                       nullEqualNull: Boolean):Unit = {
    // TODO:the following codes can be write in a single function like [calculateAttributeClusters]
    val attributeClusters = new Array[HashMap[Int,ArrayBuffer[Int]]](numAttributes)

    for(i <- 0 until numAttributes){
      attributeClusters(i) = new mutable.HashMap[Int, ArrayBuffer[Int]]()
    }
    var numRecords = 0
    val dataSet = iter.toArray
    for(line <- dataSet){
      if(line.size == numAttributes){
        for(i <- 0 until numAttributes ){//add condition to remove illegal input
        val attributeMap = attributeClusters(i)
          val cluster = attributeMap.getOrElseUpdate(line(i), new ArrayBuffer[Int]())
          cluster += numRecords
        }
      }
      numRecords += 1
      if(numRecords == Integer.MAX_VALUE - 1)
        throw new RuntimeException("Pli encoding into integer based PLIs is not possible, because the number of record is large than the Integer. Please use [Long] instead of [Int].")
    }
    val plis = fetchPositionListIndexes(attributeClusters, nullEqualNull)
    this.plis = plis
    this.compressedRecords = dataSet
  }

  //calculate data structure
  def getDataContainer(dataSet: Array[Array[Int]],
                       numAttributes:Int,
                       inputFileSeparator: String,
                       nullEqualNull: Boolean):Unit = {
    // TODO:the following codes can be write in a single function like [calculateAttributeClusters]
    val attributeClusters = new Array[HashMap[Int,ArrayBuffer[Int]]](numAttributes)

    for(i <- 0 until numAttributes){
      attributeClusters(i) = new mutable.HashMap[Int, ArrayBuffer[Int]]()
    }
    var numRecords = 0
    for(line <- dataSet){
      if(line.size == numAttributes){
        for(i <- 0 until numAttributes ){//add condition to remove illegal input
        val attributeMap = attributeClusters(i)
          val cluster = attributeMap.getOrElseUpdate(line(i), new ArrayBuffer[Int]())
          cluster += numRecords
        }
      }
      numRecords += 1
      if(numRecords == Integer.MAX_VALUE - 1)
        throw new RuntimeException("Pli encoding into integer based PLIs is not possible, because the number of record is large than the Integer. Please use [Long] instead of [Int].")
    }

    val plis = fetchPositionListIndexes(attributeClusters, nullEqualNull)
    this.plis = plis
    this.compressedRecords = dataSet
  }

  /***
    * Calculate plis
    *
    * @param attributeClusters clusters of each attribute
    * @param nullEqualNull is null equal null
    * @return clusters of each attribute after removing cluster those size is less than one
    */
  private def fetchPositionListIndexes(attributeClusters: Array[HashMap[Int,ArrayBuffer[Int]]],
                                       nullEqualNull: Boolean): Array[PositionListIndex] ={

    val clustersPerAttribute = new ArrayBuffer[PositionListIndex]()

    for(i<- 0 until attributeClusters.size){
      val clusterBuffer:ArrayBuffer[Array[Int]] = new ArrayBuffer[Array[Int]]()
      val attributeMap = attributeClusters(i)
      for((_,cluster) <- attributeMap)  {
        if(cluster.size > 1) clusterBuffer += cluster.toArray
      }

      val positionListIndex = new PositionListIndex(i, clusterBuffer.toArray)
      clustersPerAttribute += positionListIndex
    }

    clustersPerAttribute.toArray
  }

}
