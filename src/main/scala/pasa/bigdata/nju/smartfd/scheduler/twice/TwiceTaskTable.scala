package pasa.bigdata.nju.smartfd.scheduler.twice

import java.util.PriorityQueue

import pasa.bigdata.nju.smartfd.scheduler.{SlaverTable, SlaverTaskTableImpl, TaskTableImpl}
import pasa.bigdata.nju.smartfd.structures.{FDTree, FDs}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class TwiceTaskTable(fDTree: FDTree,
                     numPartitions: Int,
                     distributedAttribute: Int,
                     numAttributes: Int,
                     efficiencyThreshold: Float)
  extends TaskTableImpl(fDTree, distributedAttribute, numAttributes) {

  private val fdQueue = new PriorityQueue[FDs](numAttributes, new FDComparator)
  private val slaverTables = new Array[SlaverTable](numPartitions)
  private var goToSampler: Boolean = false
  private var numOfFDs: Int = 0
  private var numOfInvalidFDs: Int = 0
  private var times: Int = 0

  def reGetTasks(nonFDs: ArrayBuffer[FDs]): (Boolean, Map[Int, Array[FDs]]) = (false, null)

  override def getTasks(nonFDs: ArrayBuffer[FDs]) = {

    if(this.level != 0){
      rmNonFds(nonFDs, fdQueue, slaverTables)
      rmNonFDsFromCurrentLevel(nonFDs.toArray)
      generateNewFDs(nonFDs.toArray)
      this.fDTree.removeNonFDs(distributedAttribute)//TODO: recalculate [depth] of the fdTree
    }

    for(nonFD <- nonFDs){
      numOfInvalidFDs += nonFD.getRhss.cardinality()
    }

    if(times == 2){
      fdQueue.clear()
      currentLevelFDs.clear()
      currentLevel.clear()
    }
    //   get candidate fds
    if(fdQueue.size() == 0){//maybe this is the first time calling [getTasks] or the current level fds' validation has been finished
      times = 0
      if(this.level == 0){//first calling. Need to get the next level fds
        this.getCurrentLevel
        this.getCurrentLevelFDs
        for((_,fds) <- currentLevelFDs){
          val lhs = fds.getLhs.get(0, numAttributes)
          val rhss = fds.getRhss.get(0, numAttributes)
          fdQueue.add(new FDs(lhs, rhss))
          numOfFDs += fds.getRhss.cardinality()
        }
        val numFdOfEachPartition = Math.ceil(fdQueue.size().toDouble / numPartitions).toInt
        for(i <- 0 until slaverTables.size){
          slaverTables(i) = new SlaverTaskTableImpl(currentLevelFDs, numAttributes, numFdOfEachPartition)
        }
      }else{//current level fds' validation has been finished. Need to do some cleaning job and get the next level fds
        if(goToSampler){//if jump to [Sampler] last time, need to get currentLevel from the root of the fdTree(Because the fdTree has been modified).
          this.goToSampler = false
          currentLevel = this.fDTree.getLevel(this.level, this.distributedAttribute)
          this.level += 1
          this.getCurrentLevelFDs
          for((_,fds) <- currentLevelFDs){
            val lhs = fds.getLhs.get(0, numAttributes)
            val rhss = fds.getRhss.get(0, numAttributes)
            fdQueue.add(new FDs(lhs, rhss))
            numOfFDs += fds.getRhss.cardinality()
          }
          val numFdOfEachPartition = Math.ceil(fdQueue.size().toDouble / numPartitions).toInt
          for(i <- 0 until slaverTables.size){
            slaverTables(i) = new SlaverTaskTableImpl(currentLevelFDs, numAttributes, numFdOfEachPartition)
          }
        }else{
          if(!validationIsEfficiency(numOfInvalidFDs, numOfFDs - numOfInvalidFDs, efficiencyThreshold)){// if validation efficiency becomes low, go to sampler
            goToSampler = true
            numOfFDs = 0
            numOfInvalidFDs = 0
          }else{
            currentLevel = this.fDTree.getLevel(this.level, this.distributedAttribute)
            this.level += 1
            this.getCurrentLevelFDs
            for((_,fds) <- currentLevelFDs){
              val lhs = fds.getLhs.get(0, numAttributes)
              val rhss = fds.getRhss.get(0, numAttributes)
              fdQueue.add(new FDs(lhs, rhss))
              numOfFDs += fds.getRhss.cardinality()
            }
            val numFdOfEachPartition = Math.ceil(fdQueue.size().toDouble / numPartitions).toInt
            for(i <- 0 until slaverTables.size){
              slaverTables(i) = new SlaverTaskTableImpl(currentLevelFDs, numAttributes, numFdOfEachPartition)
            }
          }
        }
      }
    }

    val assignment = new mutable.HashMap[Int, ArrayBuffer[FDs]]()

    if(times == 0){
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
      times += 1
    }else if(times == 1){
      for(i <- 0 until slaverTables.size){
        val slaverTask = new ArrayBuffer[FDs]()
        val candidateFDs = if(slaverTables(i).isInstanceOf[SlaverTaskTableImpl]){
          slaverTables(i).asInstanceOf[SlaverTaskTableImpl].getValidateTasks
        }else{
          null
        }
        assert(candidateFDs != null, "Wrong type of slaverTask")
        for((_, fds) <- candidateFDs){
          slaverTask.+=(fds)
        }
        assignment += (i -> slaverTask)
      }
      times += 1
    }
    val newAssignment = assignment.map(f => (f._1, f._2.toArray)).toMap
    (goToSampler, newAssignment)
  }

}
