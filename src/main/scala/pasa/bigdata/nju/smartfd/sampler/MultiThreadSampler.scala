package pasa.bigdata.nju.smartfd.sampler

import java.util
import java.util.PriorityQueue
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

import pasa.bigdata.nju.smartfd.conf.Conf
import pasa.bigdata.nju.smartfd.structures.{FDList, FDSet, PositionListIndex}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.ExecutionException

class MultiThreadSampler extends AbstractSampler with Serializable {
  def this(conf: Conf, dataSet: Iterator[Array[Int]]){
    this
    this.efficiencyThreshold = conf.efficiencyThreshold
    this.negCover = new FDSet(conf.numAttributes)
    this.numAttributes = conf.numAttributes
    this.conf = conf
    getDataContainer(dataSet, conf.numAttributes, conf.inputFileSeparator, conf.nullEqualsNull)
  }

  def this(conf: Conf, dataSet: Array[Array[Int]]){
    this
    this.efficiencyThreshold = conf.efficiencyThreshold
    this.negCover = new FDSet(conf.numAttributes)
    this.numAttributes = conf.numAttributes
    this.conf = conf
    getDataContainer_v1(dataSet, conf.numAttributes, conf.inputFileSeparator, conf.nullEqualsNull)
  }

  private var attributeRepresentants: util.ArrayList[AttributeRepresentant] = null
  private var queue: PriorityQueue[AttributeRepresentant] = null
  private var compressedRecords: Array[Array[Int]] = null
  private var plis: Array[PositionListIndex]= null
  private var efficiencyThreshold: Float = 0.0f
  private var negCover: FDSet =  null
  private var numAttributes: Int = 0
  private var conf: Conf = null

  /**
    * get the sample data for each partition
    * @return invalid fds(sample result)
    */
  override def takeSample(): FDList = {
    val newNonFds: FDList = new FDList(numAttributes)
    if(this.plis != null && this.compressedRecords != null
      && this.compressedRecords.size > 0 && this.compressedRecords(0).size > 0){

      if (this.attributeRepresentants == null) { // if this is the first call of this method
        val sortExecutor = Executors.newFixedThreadPool(conf.numOfThreadUsedInSampler)
        for(i <- 0 until plis.size){
          val comparator: ClusterOrdering = new ClusterOrdering(compressedRecords, (i - 1 + numAttributes) % numAttributes, (i + 1) % numAttributes)
          val clusters = plis(i).getClusters()
          sortExecutor.submit(new ClusterSorter(comparator, clusters))
        }
        sortExecutor.shutdown()
        while(!sortExecutor.isTerminated){}

        val sampleExecutor = Executors.newFixedThreadPool(conf.numOfThreadUsedInSampler)
        val results = new ArrayBuffer[Future[mutable.HashSet[util.BitSet]]]()
        this.attributeRepresentants = new util.ArrayList[AttributeRepresentant](numAttributes)
        this.queue = new PriorityQueue[AttributeRepresentant](numAttributes)
        for(i <- 0 until numAttributes){
          val attributeRepresentant: AttributeRepresentant = new AttributeRepresentant(plis(i).getClusters, negCover, numAttributes, i, compressedRecords)
          results += (sampleExecutor.submit(new SampleTask(attributeRepresentant, negCover)))
          this.attributeRepresentants.add(attributeRepresentant)
        }
        getSampleData(results, negCover, newNonFds, attributeRepresentants, sampleExecutor)
        if (!(this.queue.isEmpty)) {
          this.efficiencyThreshold = Math.min(0.01f, this.queue.peek.getEfficiency * 0.5f) // This is an optimization that we added after writing the HyFD paper
        }
      } else { // Decrease the efficiency threshold
        if (!(this.queue.isEmpty)) {
          this.efficiencyThreshold = Math.min(this.efficiencyThreshold / 2, this.queue.peek.getEfficiency * 0.9f)
        }
      }

      this.attributeRepresentants = new util.ArrayList[AttributeRepresentant](queue.size())
      val sampleExecutor = Executors.newFixedThreadPool(conf.numOfThreadUsedInSampler)
      val results = new ArrayBuffer[Future[mutable.HashSet[util.BitSet]]]()
      while (!(this.queue.isEmpty) && (this.queue.peek.getEfficiency >= this.efficiencyThreshold)) {
        val attributeRepresentant: AttributeRepresentant = this.queue.poll()
        attributeRepresentants.add(attributeRepresentant)
        results += sampleExecutor.submit(new SampleTask(attributeRepresentant, negCover))
      }
      getSampleData(results, negCover, newNonFds, attributeRepresentants, sampleExecutor)
    }
    newNonFds
  }

  /**
    * get sample result
    * @param results invalid fds
    * @param negCover historical sampling data
    * @param newNonFds new sample data
    * @param attributeRepresentants sample data structure
    * @param sampleExecutor sample task(future)
    */
  private def getSampleData(results: ArrayBuffer[Future[mutable.HashSet[util.BitSet]]],
                            negCover: FDSet,
                            newNonFds: FDList,
                            attributeRepresentants: util.ArrayList[AttributeRepresentant],
                            sampleExecutor: ExecutorService): Unit ={
    try{
      for(i <- 0 until results.size){
        for(bitSet <- results(i).get()){
          if(!negCover.contains(bitSet)){
            newNonFds.add(bitSet)
            negCover.add(bitSet)
          }
        }
        if (attributeRepresentants.get(i).getEfficiency > 0.0f) {
          this.queue.add(attributeRepresentants.get(i)) // If the efficiency is 0, the algorithm will never schedule a next run for the attribute regardless how low we set the efficiency threshold
        }
      }
    }catch {
      case ex: InterruptedException => ex.printStackTrace()
      case ex: ExecutionException => ex.printStackTrace()
    }finally{
      sampleExecutor.shutdown()
    }
    while(!sampleExecutor.isTerminated){}
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
  private class AttributeRepresentant extends Comparable[AttributeRepresentant] {
    def this(clusters: Array[Array[Int]],
             negCover: FDSet,
             numAttributes: Int,
             attributeIndex: Int,
             compressedRecords: Array[Array[Int]]){
      this
      this.clusters = clusters
      //this.negCover = new FDSet(numAttributes)
      this.numAttributes = numAttributes
      this.attributeIndex = attributeIndex
      this.compressedRecords = compressedRecords
    }

    private var windowDistance: Int = 0
    private val numNewNonFds: util.ArrayList[Int] = new util.ArrayList[Int]()
    private val numComparisons: util.ArrayList[Int] = new util.ArrayList[Int]()
    //private var negCover: FDSet = null
    private var clusters: Array[Array[Int]] = null
    private var numAttributes: Int = 0
    private var compressedRecords: Array[Array[Int]] = null
    var attributeIndex: Int = 0

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
      * @param negCover
      * @return
      */
    def runNext(negCover: FDSet): mutable.HashSet[util.BitSet] = {
      //println("in runNext...")
      val newNonFds: mutable.HashSet[util.BitSet] = new mutable.HashSet[util.BitSet]()
      this.windowDistance += 1
      var curNumComparisons: Int = 0
      var clusterIndex = 0
      while(clusterIndex < clusters.length){
        val cluster = clusters(clusterIndex)
        if (cluster.size > windowDistance) {
          var recordIndex: Int = 0
          while (recordIndex < (cluster.size - windowDistance)) {
            val recordId: Int = cluster(recordIndex)
            val partnerRecordId: Int = cluster(recordIndex + windowDistance)
            val equalAttrs: java.util.BitSet = new util.BitSet(numAttributes)
            matches(equalAttrs, compressedRecords(recordId), compressedRecords(partnerRecordId))
            if (!(negCover.contains(equalAttrs))) {
              newNonFds += equalAttrs
            }
            curNumComparisons += 1
            recordIndex += 1
          }
        }
        clusterIndex += 1
      }
      numNewNonFds.add(newNonFds.size)
      numComparisons.add(curNumComparisons)
      newNonFds
    }
  }


  private def matches(equalAttrs: java.util.BitSet, t1: Array[Int], t2: Array[Int]): Unit = {
    var index = 0
    while(index < t1.length){
      if ((t1(index) == t2(index))) {
        equalAttrs.set(index)
      }
      index += 1
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
  def getDataContainer_v1(dataSet: Array[Array[Int]],
                          numAttributes:Int,
                          inputFileSeparator: String,
                          nullEqualNull: Boolean):Unit = {
    // TODO:the following codes can be write in a single function like [calculateAttributeClusters]
    val attributeClusters = new Array[HashMap[Int,ArrayBuffer[Int]]](numAttributes)

    for(i <- 0 until numAttributes){
      attributeClusters(i) = new mutable.HashMap[Int, ArrayBuffer[Int]]()
    }
    var numRecords = 0
    var i = 0
    while(i < dataSet.size){
      val line = dataSet(i)
      if(line.size == numAttributes){
        var j = 0
        while(j < numAttributes){
          val attributeMap = attributeClusters(j)
          val cluster = attributeMap.getOrElseUpdate(line(j), new ArrayBuffer[Int]())
          cluster += numRecords
          j += 1
        }
      }
      numRecords += 1
      if(numRecords == Integer.MAX_VALUE - 1)
        throw new RuntimeException("Pli encoding into integer based PLIs is not possible, because the number of record is large than the Integer. Please use [Long] instead of [Int].")
      i += 1
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
    var i = 0
    while(i < attributeClusters.size){
      val clusterBuffer:ArrayBuffer[Array[Int]] = new ArrayBuffer[Array[Int]]()
      val attributeMap = attributeClusters(i)

      for((_,cluster) <- attributeMap)  {
        if(cluster.size > 1) clusterBuffer += cluster.toArray
      }

      val positionListIndex = new PositionListIndex(i, clusterBuffer.toArray)
      clustersPerAttribute += positionListIndex
      i += 1
    }

    clustersPerAttribute.toArray
  }

  /**
    * sort clusters
    * @param order order that used to sort a data
    * @param clusters data to be sorted
    */
  private class ClusterSorter(order: Ordering[Int], clusters: Array[Array[Int]]) extends Runnable {
    override def run(): Unit = {
      for(j <- 0 until clusters.size){
        clusters(j) = clusters(j).sorted(order)
      }
    }
  }

  /**
    * sample task
    * @param attributeRepresentant
    * @param negCover
    */
  private class SampleTask(attributeRepresentant: AttributeRepresentant, negCover: FDSet) extends Callable[mutable.HashSet[util.BitSet]]{
    override def call(): mutable.HashSet[util.BitSet] = {
      attributeRepresentant.runNext(negCover)
    }
  }
}
