package pasa.bigdata.nju.smartfd.scheduler

import java.util.{Comparator, PriorityQueue}

import pasa.bigdata.nju.smartfd.structures.FDs

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object NonFDDetection {
  def assignTask(tasks:Array[FDs], numPartitions: Int, numAttributes: Int): Map[Int, Array[FDs]] = {
    val fdQueue = new PriorityQueue[FDs](numAttributes, new FDComparator)
    val tasksPerPartition = Math.ceil(tasks.size.toDouble / numPartitions).toInt
    for(task <- tasks) fdQueue.add(task)
    val assignment = new mutable.HashMap[Int, ArrayBuffer[FDs]]()
    val slaverTables = new Array[SlaverTable](numPartitions)
    for(i <- 0 until slaverTables.size){
      slaverTables(i) = new SlaverTaskTableImpl(tasks, numAttributes, tasksPerPartition)
    }

    while(!(fdQueue.isEmpty)){//assign task
    val currentCandidateFD = fdQueue.poll()
      var slaveNum: Int = 0
      while(slaveNum < numPartitions && !slaverTables(slaveNum).getTasks(currentCandidateFD)){
        slaveNum += 1
      }
      if(slaveNum < numPartitions){
        val currentCandidateFDCopy = new FDs(currentCandidateFD.getLhs.get(0, numAttributes), currentCandidateFD.getRhss.get(0, numAttributes))
        if(assignment.contains(slaveNum)){
          assignment(slaveNum).+=(currentCandidateFDCopy)
          currentCandidateFD.increaseValidateFlag
          fdQueue.add(currentCandidateFD)
        }else{
          val slaveTask = new ArrayBuffer[FDs]()
          slaveTask.+=(currentCandidateFDCopy)
          assignment += (slaveNum -> slaveTask)
          currentCandidateFD.increaseValidateFlag
          fdQueue.add(currentCandidateFD)
        }
      }
    }

    val newAssignment = assignment.map(f => (f._1, f._2.toArray)).toMap
    newAssignment
  }

  class FDComparator extends Comparator[FDs]{
    override def compare(fd1: FDs, fd2: FDs):Int = {
      if(fd1.validateFlag < fd2.validateFlag){
        -1
      }else if(fd1.validateFlag > fd2.validateFlag){
        1
      }else{
        0
      }
    }
  }
}


