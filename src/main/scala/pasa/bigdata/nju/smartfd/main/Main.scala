package pasa.bigdata.nju.smartfd.main

import java.io.{File, PrintWriter}
import java.net.URI
import java.util
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}
import pasa.bigdata.nju.smartfd.conf.Conf
import pasa.bigdata.nju.smartfd.distributor.Distributor
import pasa.bigdata.nju.smartfd.inductor.{InductorInterface, MultiThreadInductor}
import pasa.bigdata.nju.smartfd.reader.Reader
import pasa.bigdata.nju.smartfd.sampler.{Sampler, SamplerRDD}
import pasa.bigdata.nju.smartfd.scheduler.onetime.OneTimeSchedulerImpl
import pasa.bigdata.nju.smartfd.scheduler.twice.TwiceSchedulerImpl
import pasa.bigdata.nju.smartfd.scheduler.{NonFDDetection, Scheduler}
import pasa.bigdata.nju.smartfd.skewproducer.SkewProducer
import pasa.bigdata.nju.smartfd.structures._
import pasa.bigdata.nju.smartfd.utils.{GetCandidateFDs, Utils}
import pasa.bigdata.nju.smartfd.validator.{MultiThreadValidator, ValidationResult, ValidatorRDD}

import scala.collection.mutable.ArrayBuffer

object Main {
def main(args: Array[String]) {
    val algorithmStartTime = System.currentTimeMillis()
    val conf = Conf.getConf(args)
    val fileName = conf.inputFilePath.split("/")
    var writer:PrintWriter = null
    if (conf.writerLocalLogs){
      val logFile = s"${conf.localLogDir}/${fileName(fileName.size-1)}_log.txt"
      writer = new PrintWriter(new File(logFile))
    }

    val sparkConf = new SparkConf().setAppName(conf.appName).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(
      classOf[Distributor],
      classOf[ValidationResult],
      classOf[Sampler],
      classOf[MultiThreadValidator],
      classOf[Conf],
      classOf[FDList],
      classOf[FDSet],
      classOf[FDTree],
      classOf[FDs],
      classOf[Array[Int]],
      classOf[ArrayBuffer[Int]],
      classOf[IntegerPair],
      classOf[java.util.BitSet]))
    val sc = new SparkContext(sparkConf)

    val reader = new Reader(conf.inputFromHBase, conf.inputFilePath, conf.numPartition, sc)
    val dataSet: RDD[String] = reader.getDataSet().cache()
    val distributor = new Distributor(conf, dataSet)
    val tempPath = conf.tempFilePath + "/fd.tmp"
    val path = new Path(tempPath)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new URI(tempPath), new org.apache.hadoop.conf.Configuration())
    if(hdfs.exists(path)) hdfs.delete(path, true)
    distributor.sortedDataSet.map(f=>f.mkString(",")).saveAsTextFile(tempPath)
    val sortedDataSet = sc.textFile(tempPath).map(f=>f.split(",").map(x=>x.toInt)).cache()
    distributor.sortedDataSet.unpersist(false)
    val posCover = new FDTree(conf.numAttributes)
    posCover.init(distributor.sortedCardinality)

    if(conf.writerLocalLogs){
      writer.write("sorted cardinality:\n")
      for(cardinality <- distributor.sortedCardinality){
        writer.write(cardinality + " ")
        writer.flush()
      }
      writer.write("\n")

      writer.write("sorted attributes:\n")
      for(attribute <- distributor.sortedAttributes){
        writer.write(attribute + " ")
        writer.flush()
      }
      writer.write("\n")
    }
    val negCover: FDSet = new FDSet(conf.numAttributes)
    val inductor: MultiThreadInductor = new MultiThreadInductor(posCover)

    var currentAttribute = 0
    val executor = Executors.newCachedThreadPool()
    val lock = new ReentrantLock()
    var flag = true
    while(currentAttribute < conf.numAttributes){
      if((posCover.getChildren != null
        && posCover.getChildren(currentAttribute) != null)
        ||posCover.isFd(currentAttribute)){
        if(flag && GetCandidateFDs.percentOfValidateFD(posCover, currentAttribute) < conf.algorithmParallelThreshold){
          if(distributor.isSkew(currentAttribute)){
            val skewProducer = new SkewProducer(sortedDataSet, inductor, conf, negCover, currentAttribute, posCover)
            skewProducer.validate()
          }else{
            val keyValueData = sortedDataSet.map(f=>(f(currentAttribute), f))
            val tempRDD = if(conf.rearrangeUsingRange){
              keyValueData.repartitionAndSortWithinPartitions(new RangePartitioner(conf.numPartition, keyValueData)).map(f=>f._2)
            }else{
              keyValueData.repartitionAndSortWithinPartitions(new HashPartitioner(conf.numPartition)).map(f=>f._2)
            }
            val validator: ValidatorRDD = new ValidatorRDD(tempRDD, conf, distributor.sortedCardinality, currentAttribute).cache()
            validator.count()
            var attributeFocusedSampler: SamplerRDD = null
            var goToSampler = true
            var assignment: Map[Int, Array[FDs]] = null
            val scheduler: Scheduler = conf.scheduleMod match {
              //case "onefd" => new OneFdSchedulerImpl(posCover, conf, currentAttribute, recordsNumberOfEachPartition)
              case "onetime" => new OneTimeSchedulerImpl(posCover, conf, currentAttribute)
              //case "twice" => new TwiceSchedulerImpl1(posCover, conf, currentAttribute)
              //case "multi" => new MultiTimeSchedulerImpl(posCover, conf, currentAttribute, recordsNumberOfEachPartition)
              //case "minibatch" =>new MiniBatchSchedulerImpl(posCover, conf, currentAttribute)
              case _ => throw new RuntimeException("No such scheduler mod: " + conf.scheduleMod)
            }
            var task = scheduler.getTasks(new ArrayBuffer[FDs]())
            goToSampler = task._1
            assignment = task._2
            var preValidateEfficiency = 0.0
            var sampleEfficiency = 0.0
            while(assignment.size != 0){
              val fdNum = (assignment(0).map(f=>f.getRhss.cardinality())).sum
              if(conf.writerLocalLogs){
                writer.write("******************************in assignment start*******************\n")
                writer.write(s"current attribute: ${currentAttribute}   lhs size: ${assignment(0)(0).getLhs.cardinality()}  task size: ${assignment(0).size}  fd numbers: ${fdNum}\n")
                writer.flush()
              }
              if(conf.useDetection && fdNum > conf.detectedThreshold || preValidateEfficiency < sampleEfficiency) {
                val detectionStart = System.currentTimeMillis()
                val detectionTask = NonFDDetection.assignTask(assignment(0), conf.numPartition, conf.numAttributes)
                val detectionResults = validator.mapPartitionsWithIndex{(index, iter)=>
                  Iterator(iter.next().validate(detectionTask(index)))
                }.collect()
                val detectionTime = System.currentTimeMillis() - detectionStart
                if(preValidateEfficiency == 0.0) preValidateEfficiency = (Math.ceil(assignment(0).size.toDouble / conf.numPartition)) / detectionTime * 1000
                if(conf.writerLocalLogs){
                  writer.write("detection time: " + detectionTime + "ms\n")
                  writer.flush()
                }
                val detectionResult = detectionResults(0)
                for(otherResult<- 1 until detectionResults.size){
                  detectionResult.add(detectionResults(otherResult))
                }
                val invalids = detectionResult.getNonFDsToBuffer
                val invalidRate = invalids.map(f => f.getRhss.cardinality()).sum.toDouble / fdNum
                if(conf.writerLocalLogs){
                  writer.write(s"In detection, invalid fd rate: " + invalidRate + "\n")
                  writer.flush()
                }
                val fDList: FDList = new FDList(conf.numAttributes)
                val fdSet = detectionResult.fdSet.iterator()
                var count = 0
                while(fdSet.hasNext){
                  val bitSet = fdSet.next()
                  if(conf.writerLocalLogs){
                    writer.write(Utils.bitSetToString(bitSet, conf.numAttributes) + "\n")
                  }
                  if(!negCover.contains(bitSet)){
                    fDList.add(bitSet)
                    negCover.add(bitSet)
                    count += 1
                  }
                }
                if(conf.writerLocalLogs){
                  writer.write("conflict data number in detector: " + count + "\n")
                }
                val inductorStart = System.currentTimeMillis()
                inductor.updatePositiveCover(fDList, currentAttribute)
                sampleEfficiency = preValidateEfficiency + 1
                if(conf.writerLocalLogs){
                  writer.write("inductor time: " + (System.currentTimeMillis() - inductorStart) + "ms\n")
                  writer.write("preValidate efficiency: " + preValidateEfficiency + "\n")
                  writer.flush()
                }
                while (invalidRate * conf.numPartition > conf.detectedToSampleThreshold && conf.useSampler && sampleEfficiency > preValidateEfficiency) {
                  if (attributeFocusedSampler == null) {
                    attributeFocusedSampler = new SamplerRDD(validator, conf).cache()
                    attributeFocusedSampler.count()
                  }
                  val sampleStart = System.currentTimeMillis()
                  val sampleResult = attributeFocusedSampler.map(f=>f.takeSample()).collect().map(f=>f.getFdLevels)
                  val sampleTime = System.currentTimeMillis() - sampleStart
                  val negCoverSize = negCover.size
                  val fDList: FDList = new FDList(conf.numAttributes)
                  addSamplerData(fDList, negCover, sampleResult)
                  if(conf.writerLocalLogs){
                    writer.write("attribute focused sample time: " + sampleTime + "ms\n")
                    writer.write("attribute focused sample data: " + (negCover.size - negCoverSize) + "\n")
                    writer.write("sample data info: \n")
                    val fdLevel = fDList.getFdLevels
                    for(i <- 0 until fdLevel.size()){
                      writer.write(i + ":" + fdLevel.get(i).size() + "\n")
                    }
                    writer.flush()
                  }
                  val inductorStart = System.currentTimeMillis()
                  val numOfRemovedNonFD = inductor.updatePositiveCover(fDList, currentAttribute)
                  sampleEfficiency = numOfRemovedNonFD.toDouble / sampleTime * 1000
                  if(conf.writerLocalLogs){
                    writer.write("sample efficiency: " + sampleEfficiency + "\n")
                    writer.write("inductor time: " + (System.currentTimeMillis() - inductorStart) + "ms\n")
                    writer.flush()
                  }
                }

                //re-get task
                task = scheduler.reGetTasks(new ArrayBuffer[FDs]())
                assignment = task._2
                if(assignment.size > 0){
                  val fdNum1 = (assignment(0).map(f=>f.getRhss.cardinality())).sum
                  if(conf.writerLocalLogs){
                    writer.write(s"After reGet task, current attribute: ${currentAttribute}   lhs size: ${assignment(0)(0).getLhs.cardinality()}  task size: ${assignment(0).size}  fd numbers: ${fdNum1}\n")
                    writer.flush()
                  }
                }
              }

              if(assignment.size > 0){
                val validatorStart = System.currentTimeMillis()
                val validateResult = new ValidationResult(0, conf.numAttributes)
                if(assignment(0).size > conf.miniBatchSize){
                  val times = Math.ceil(assignment(0).size.toDouble / conf.miniBatchSize).toInt
                  for(i <- 0 until times){
                    val validateResults = validator.mapPartitionsWithIndex{(index, iter)=>
                      Iterator(iter.next().validate(assignment(index).slice(i * conf.miniBatchSize, Math.min((i + 1) * conf.miniBatchSize, assignment(index).size))))
                    }.collect()
                    for(otherResult<- 0 until validateResults.size){
                      validateResult.add(validateResults(otherResult))
                    }
                  }
                }else{
                  val validateResults = validator.mapPartitionsWithIndex{(index, iter)=>
                    Iterator(iter.next().validate(assignment(index)))
                  }.collect()
                  for(otherResult<- 0 until validateResults.size){
                    validateResult.add(validateResults(otherResult))
                  }
                }

                preValidateEfficiency = assignment(0).size.toDouble / (System.currentTimeMillis() - validatorStart) * 1000
                val invalidFDs = validateResult.getNonFDsToBuffer
                if(conf.writerLocalLogs){
                  writer.write("validator time: " + (System.currentTimeMillis() - validatorStart) + "ms\n")
                  writer.write("invalid fd numbers: " + invalidFDs.map(f=>f.getRhss.cardinality()).sum + "\n")
                }
                val fdNum = (assignment(0).map(f=>f.getRhss.cardinality())).sum
                val validateRate = 1 - ((invalidFDs.map(f=>f.getRhss.cardinality())).sum.toDouble / fdNum)
                if(conf.writerLocalLogs){
                  writer.write("validate rate: " + validateRate + "\n")
                  writer.flush()
                }

                val fDList = new FDList(conf.numAttributes)
                val fdSet = validateResult.fdSet.iterator()
                var count = 0
                while(fdSet.hasNext){
                  val bitSet = fdSet.next()
                  if(conf.writerLocalLogs){
                    writer.write(Utils.bitSetToString(bitSet, conf.numAttributes) + "\n")
                  }
                  if(!negCover.contains(bitSet)){
                    negCover.add(bitSet)
                    fDList.add(bitSet)
                    count += 1
                  }
                }
                if(conf.writerLocalLogs){
                  writer.write("conflict data number in validator: " + count + "\n")
                }
                val inductorStart = System.currentTimeMillis()
                inductor.updatePositiveCover(fDList, currentAttribute)
                if(conf.writerLocalLogs){
                  writer.write("inductor time: " + (System.currentTimeMillis() - inductorStart) + "ms\n")
                  writer.flush()
                }
                assignment = scheduler.getTasks(new ArrayBuffer[FDs]())._2
              }
              if(conf.writerLocalLogs){
                writer.write("**************************assignment one run end*******************\n")
                writer.flush()
              }
            }
            validator.unpersist()
            if(attributeFocusedSampler != null) attributeFocusedSampler.unpersist()
          }
        }else{
          flag = false
          executor.submit(new CalculateEachAttribute(currentAttribute, posCover, distributor, conf, lock, inductor, sortedDataSet, negCover))
        }
      }
      currentAttribute += 1
    }

    executor.shutdown()
    while(!executor.isTerminated){}

    writeResultToHDFS(conf, GetCandidateFDs.getMinimalFDsForParUsingColumnId(posCover, distributor.sortedAttributes))
    if(conf.writerLocalLogs){
      writer.write(s"${conf.inputFilePath} takes time: ${(System.currentTimeMillis() - algorithmStartTime).toDouble / 1000}s\n")
      writer.close()
    }
    println(s"Total Time: ${(System.currentTimeMillis() - algorithmStartTime).toDouble / 1000}s")
  }

  def writeResultToHDFS(conf: Conf, result: Array[String]): Unit ={
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(URI.create(conf.outputFilePath + "/part-00000"), hdfsConf)
    val hdfsOutStream = fs.create(new Path(conf.outputFilePath + "/part-00000"))
    var i = 0
    while(i < result.size){
      hdfsOutStream.writeChars(result(i) + "\n")
      i += 1
    }
    hdfsOutStream.close()
  }
  def addSamplerData(fDList: FDList,
                     negCover: FDSet,
                     sampleResults: Array[util.ArrayList[util.ArrayList[util.BitSet]]]): Unit ={
    var resultIndex = 0
    while(resultIndex < sampleResults.size){
      val fdLevel = sampleResults(resultIndex)
      var i = 0
      while(i < fdLevel.size()){
        val level = fdLevel.get(i)
        var j = 0
        while(j < level.size()){
          if(!negCover.contains(level.get(j))){
            negCover.add(level.get(j))
            fDList.add(level.get(j))
          }
          j += 1
        }
        i += 1
      }
      resultIndex += 1
    }

  }

  class CalculateEachAttribute() extends Runnable with Serializable {

    def this(currentAttribute: Int,
             posCover: FDTree,
             distributor: Distributor,
             conf: Conf,
             lock: ReentrantLock,
             inductor: InductorInterface,
             sortedDataSet: RDD[Array[Int]],
             negCover: FDSet) {
      this
      this.currentAttribute = currentAttribute
      this.posCover = posCover
      this.negCover = negCover
      this.distributor = distributor
      this.conf = conf
      this.lock = lock
      this.inductor = inductor
      this.sortedDataSet = sortedDataSet
    }

    var currentAttribute: Int = 0
    @transient
    var posCover: FDTree = null
    @transient
    var negCover: FDSet = null
    @transient
    var distributor: Distributor = null
    var conf: Conf = null
    @transient
    var lock: ReentrantLock = null
    @transient
    var inductor: InductorInterface = null
    var sortedDataSet: RDD[Array[Int]] = null

    override def run(): Unit = {
      if(distributor.isSkew(currentAttribute)){
        val skewProducer = new SkewProducer(sortedDataSet, inductor, conf, negCover, currentAttribute, posCover)
        skewProducer.validate()
      }else{
        val keyValueData = sortedDataSet.map(f=>(f(currentAttribute), f))
        val tempRDD = if(conf.rearrangeUsingRange){
          keyValueData.repartitionAndSortWithinPartitions(new RangePartitioner(conf.numPartition / 2, keyValueData)).map(f=>f._2)
        }else{
          keyValueData.repartitionAndSortWithinPartitions(new HashPartitioner(conf.numPartition / 2)).map(f=>f._2)
        }
        val validator: ValidatorRDD = new ValidatorRDD(tempRDD, conf, distributor.sortedCardinality, currentAttribute).cache()
        validator.count()
        var attributeFocusedSampler: SamplerRDD = null
        var goToSampler = true
        var assignment: Map[Int, Array[FDs]] = null
        val scheduler: Scheduler = conf.scheduleMod match {
          //case "onefd" => new OneFdSchedulerImpl(posCover, conf, currentAttribute, recordsNumberOfEachPartition)
          case "onetime" => new OneTimeSchedulerImpl(posCover, conf, currentAttribute)
          case "twice" => new TwiceSchedulerImpl(posCover, conf, currentAttribute)
          //case "multi" => new MultiTimeSchedulerImpl(posCover, conf, currentAttribute, recordsNumberOfEachPartition)
          case _ => throw new RuntimeException("No such scheduler mod: " + conf.scheduleMod)
        }
        lock.lock()
        var task = scheduler.getTasks(new ArrayBuffer[FDs]())
        lock.unlock()
        goToSampler = task._1
        assignment = task._2

        var preValidateEfficiency = 0.0
        var sampleEfficiency = 0.0
        while(assignment.size != 0){
          val fdNum = (assignment(0).map(f=>f.getRhss.cardinality())).sum
          if(conf.useDetection && fdNum > conf.detectedThreshold || preValidateEfficiency < sampleEfficiency) {
            val detectionStart = System.currentTimeMillis()
            val detectionTask = NonFDDetection.assignTask(assignment(0), conf.numPartition, conf.numAttributes)
            val detectionResults = validator.mapPartitionsWithIndex{(index, iter)=>
              Iterator(iter.next().validate(detectionTask(index)))
            }.collect()
            val detectionTime = System.currentTimeMillis() - detectionStart
            if(preValidateEfficiency == 0.0) preValidateEfficiency = (Math.ceil(assignment(0).size.toDouble / conf.numPartition)) / detectionTime * 1000
            val detectionResult = detectionResults(0)
            for(otherResult<- 1 until detectionResults.size){
              detectionResult.add(detectionResults(otherResult))
            }
            val invalids = detectionResult.getNonFDsToBuffer
            val invalidRate = invalids.map(f => f.getRhss.cardinality()).sum.toDouble / fdNum
            val fDList: FDList = new FDList(conf.numAttributes)
            val fdSet = detectionResult.fdSet.iterator()
            lock.lock()
            while(fdSet.hasNext){
              val bitSet = fdSet.next()
              fDList.add(bitSet)
            }
            inductor.updatePositiveCover(fDList, currentAttribute)
            lock.unlock()
            sampleEfficiency = preValidateEfficiency + 1
            while (invalidRate * conf.numPartition > conf.detectedToSampleThreshold && conf.useSampler && sampleEfficiency > preValidateEfficiency) {
              if (attributeFocusedSampler == null) {
                attributeFocusedSampler = new SamplerRDD(validator, conf).cache()
                attributeFocusedSampler.count()
              }
              val sampleStart = System.currentTimeMillis()
              val sampleResult = attributeFocusedSampler.map(f=>f.takeSample()).collect().map(f=>f.getFdLevels)
              val sampleTime = System.currentTimeMillis() - sampleStart
              val fDList: FDList = new FDList(conf.numAttributes)
              lock.lock()
              addSamplerData(fDList, negCover, sampleResult)
              lock.unlock()
              lock.lock()
              val numOfRemovedNonFD = inductor.updatePositiveCover(fDList, currentAttribute)
              lock.unlock()
              sampleEfficiency = numOfRemovedNonFD.toDouble / sampleTime * 1000
            }

            lock.lock()
            task = scheduler.reGetTasks(new ArrayBuffer[FDs]())
            lock.unlock()
            assignment = task._2
          }

          if(assignment.size > 0){
            val validatorStart = System.currentTimeMillis()
            val validateResult = new ValidationResult(0, conf.numAttributes)
            if(assignment(0).size > conf.miniBatchSize){
              val times = Math.ceil(assignment(0).size.toDouble / conf.miniBatchSize).toInt
              for(i <- 0 until times){
                val validateResults = validator.mapPartitionsWithIndex{(index, iter)=>
                  Iterator(iter.next().validate(assignment(index).slice(i * conf.miniBatchSize, Math.min((i + 1) * conf.miniBatchSize, assignment(index).size))))
                }.collect()
                for(otherResult<- 0 until validateResults.size){
                  validateResult.add(validateResults(otherResult))
                }
              }
            }else{
              val validateResults = validator.mapPartitionsWithIndex{(index, iter)=>
                Iterator(iter.next().validate(assignment(index)))
              }.collect()
              for(otherResult<- 0 until validateResults.size){
                validateResult.add(validateResults(otherResult))
              }
            }
            preValidateEfficiency = assignment(0).size.toDouble / (System.currentTimeMillis() - validatorStart) * 1000
            val invalidFDs = validateResult.getNonFDsToBuffer
            val fdNum = (assignment(0).map(f=>f.getRhss.cardinality())).sum
            val fDList = new FDList(conf.numAttributes)
            val fdSet = validateResult.fdSet.iterator()
            lock.lock()
            while(fdSet.hasNext){
              val bitSet = fdSet.next()
              fDList.add(bitSet)
            }
            inductor.updatePositiveCover(fDList, currentAttribute)
            assignment = scheduler.getTasks(new ArrayBuffer[FDs]())._2
            lock.unlock()
          }
        }
        validator.unpersist()
        if(attributeFocusedSampler != null) attributeFocusedSampler.unpersist()
      }
    }
  }
}
