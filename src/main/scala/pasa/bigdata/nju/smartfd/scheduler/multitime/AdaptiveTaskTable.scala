package pasa.bigdata.nju.smartfd.scheduler.multitime

import java.util.PriorityQueue

import pasa.bigdata.nju.smartfd.scheduler.{SlaverTable, SlaverTaskTableImpl, TaskTableImpl}
import pasa.bigdata.nju.smartfd.structures.{FDTree, FDs}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class AdaptiveTaskTable (fDTree: FDTree,
                         numPartitions: Int,
                         distributedAttribute: Int,
                         numAttributes: Int,
                         efficiencyThreshold: Float,
                         adaptiveSchedulerThreshold: Int)
  extends TaskTableImpl(fDTree, distributedAttribute, numAttributes) {

  //a high priority assigned to FD with fewer validation times
  private val fdQueue = new PriorityQueue[FDs](numAttributes, new FDComparator)
  private val slaverTable = new Array[SlaverTable](numPartitions)
  private var goToSampler: Boolean = false
  private var numOfFDs: Int = 0
  private var numOfInvalidFDs: Int = 0

  def reGetTasks(nonFDs: ArrayBuffer[FDs]): (Boolean, Map[Int, Array[FDs]]) = {
    if(currentLevelFDs.size > 0) currentLevelFDs.clear()
    if(fdQueue.size() > 0) fdQueue.clear()
    assert(this.level != 0, "In OneTimeTaskTable, wrong level zero!!!")
    this.level -= 1
    rmNonFDsFromCurrentLevel(nonFDs.toArray)
    generateNewFDs(nonFDs.toArray)
    this.fDTree.removeNonFDs(distributedAttribute)//TODO: recalculate [depth] of the fdTree

    currentLevel = this.fDTree.getLevel(this.level, this.distributedAttribute)
    this.level += 1
    this.getCurrentLevelFDs

    for((_,fds) <- currentLevelFDs){
      val lhs = fds.getLhs.get(0, numAttributes)
      val rhss = fds.getRhss.get(0, numAttributes)
      fdQueue.add(new FDs(lhs, rhss))
      numOfFDs += fds.getRhss.cardinality()
    }

    val capacity = Math.ceil(fdQueue.size().toDouble / numPartitions).toInt
    for(i <- 0 until slaverTable.size){
      slaverTable(i) = new SlaverTaskTableImpl(currentLevelFDs, numAttributes, capacity)
    }

    val fdNumber = currentLevelFDs.map(f=>f._2.getRhss.cardinality()).sum
    val assignment = new mutable.HashMap[Int, ArrayBuffer[FDs]]()
    if(fdNumber < adaptiveSchedulerThreshold){
      for(eachSlaveTable <- slaverTable){
        eachSlaveTable.setCapacity(adaptiveSchedulerThreshold)
      }
    }

    while(!(fdQueue.isEmpty)){//assign task
    val currentCandidateFD = fdQueue.poll()
      var slaveNum: Int = 0
      while(slaveNum < numPartitions && !slaverTable(slaveNum).getTasks(currentCandidateFD)){
        slaveNum += 1
      }
      if(slaveNum < numPartitions){
        val lhs = currentCandidateFD.getLhs.get(0, numAttributes)
        val rhss = currentCandidateFD.getRhss.get(0, numAttributes)
        val currentCandidateFDCopy = new FDs(lhs, rhss)
        val slaveTask = assignment.getOrElseUpdate(slaveNum, new ArrayBuffer[FDs]())
        slaveTask += currentCandidateFDCopy
        currentCandidateFD.increaseValidateFlag
        fdQueue.add(currentCandidateFD)
      }
    }

    val newAssignment = assignment.map(f=>(f._1, f._2.toArray)).toMap
    (goToSampler, newAssignment)
  }

  override def getTasks(nonFDs: ArrayBuffer[FDs]) = {
    assert(nonFDs != null, "nonFDs must be initialized")
    for(nonFD <- nonFDs){
      numOfInvalidFDs += nonFD.getRhss.cardinality()
    }

    if(this.level != 0){
      rmNonFds(nonFDs, fdQueue, slaverTable)
      this.rmNonFDsFromCurrentLevel(nonFDs.toArray)
      this.generateNewFDs(nonFDs.toArray)
      this.fDTree.removeNonFDs(distributedAttribute)//TODO: recalculate [depth] of the fdTree
    }

    //get candidate fds
    if(fdQueue.isEmpty){//maybe this is the first time calling [getTasks] or the current level fds' validation has been finished
      if(this.level == 0){//first calling. Need to get the next level fds
        this.getCurrentLevel
        this.getCurrentLevelFDs
        for((_,fds) <- currentLevelFDs){
          val lhs = fds.getLhs.get(0, numAttributes)
          val rhss = fds.getRhss.get(0, numAttributes)
          fdQueue.add(new FDs(lhs, rhss))
          numOfFDs += fds.getRhss.cardinality()
        }
        val capacity = Math.ceil(fdQueue.size().toDouble / numPartitions).toInt
        for(i <- 0 until slaverTable.size){
          slaverTable(i) = new SlaverTaskTableImpl(currentLevelFDs, numAttributes, capacity)
        }
      }else{//current level fds' validation has been finished. Need to do some cleaning job and get the next level fds

        if(goToSampler){//if jump to [Sampler] last time, need to get currentLevel from the root of the fdTree(Because the fdTree has been modified).
          this.goToSampler = false
          currentLevel = this.fDTree.getLevel(this.level, this.distributedAttribute)
          this.level += 1
          currentLevelFDs.clear()
          this.getCurrentLevelFDs
          for((_,fds) <- currentLevelFDs){
            val lhs = fds.getLhs.get(0, numAttributes)
            val rhss = fds.getRhss.get(0, numAttributes)
            fdQueue.add(new FDs(lhs, rhss))
            numOfFDs += fds.getRhss.cardinality()
          }
          val capacity = Math.ceil(fdQueue.size().toDouble / numPartitions).toInt
          for(i <- 0 until slaverTable.size){
            slaverTable(i) = new SlaverTaskTableImpl(currentLevelFDs, numAttributes, capacity)
          }
        }else{
          if(!validationIsEfficiency(numOfInvalidFDs, numOfFDs - numOfInvalidFDs, efficiencyThreshold)){// if validation efficiency becomes low, go to sampler
            goToSampler = true
            numOfFDs = 0
            numOfInvalidFDs = 0
          }else{
            currentLevel = this.fDTree.getLevel(this.level, this.distributedAttribute)
            this.level += 1
            currentLevelFDs.clear()
            this.getCurrentLevelFDs
            for((_,fds) <- currentLevelFDs){
              val lhs = fds.getLhs.get(0, numAttributes)
              val rhss = fds.getRhss.get(0, numAttributes)
              fdQueue.add(new FDs(lhs, rhss))
              numOfFDs += fds.getRhss.cardinality()
            }
            val capacity = Math.ceil(fdQueue.size().toDouble / numPartitions).toInt
            for(i <- 0 until slaverTable.size){
              slaverTable(i) = new SlaverTaskTableImpl(currentLevelFDs, numAttributes, capacity)
            }
          }
        }
      }
    }

    val assignment = new mutable.HashMap[Int, ArrayBuffer[FDs]]()
    for(eachSlaveTable <- slaverTable){
      eachSlaveTable.setCapacity(adaptiveSchedulerThreshold)
    }

    while(!(fdQueue.isEmpty)){//assign task
    val currentCandidateFD = fdQueue.poll()
      var slaveNum: Int = 0
      while(slaveNum < numPartitions && !slaverTable(slaveNum).getTasks(currentCandidateFD)){
        slaveNum += 1
      }
      if(slaveNum < numPartitions){
        val lhs = currentCandidateFD.getLhs.get(0, numAttributes)
        val rhss = currentCandidateFD.getRhss.get(0, numAttributes)
        val currentCandidateFDCopy = new FDs(lhs, rhss)
        val slaveTask = assignment.getOrElseUpdate(slaveNum, new ArrayBuffer[FDs]())
        slaveTask += currentCandidateFDCopy
        currentCandidateFD.increaseValidateFlag
        fdQueue.add(currentCandidateFD)
      }
    }

    val newAssignment = assignment.map(f=>(f._1, f._2.toArray)).toMap
    (goToSampler, newAssignment)
  }

}
