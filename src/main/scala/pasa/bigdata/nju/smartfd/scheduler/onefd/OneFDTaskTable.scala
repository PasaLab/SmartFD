package pasa.bigdata.nju.smartfd.scheduler.onefd

import java.util.PriorityQueue

import pasa.bigdata.nju.smartfd.scheduler.{SlaverTable, SlaverTaskTableImpl, TaskTableImpl}
import pasa.bigdata.nju.smartfd.structures.{FDTree, FDs}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class OneFDTaskTable(fDTree: FDTree,
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

  def reGetTasks(nonFDs: ArrayBuffer[FDs]): (Boolean, Map[Int, Array[FDs]]) = (false, null)

  override def getTasks(nonFDs: ArrayBuffer[FDs]) = {
    assert(nonFDs != null, "nonFDs must be initialized")
    for(nonFD <- nonFDs){
      numOfInvalidFDs += nonFD.getRhss.cardinality()
    }

    if(this.level != 0){
      this.rmNonFDsFromCurrentLevel(nonFDs.toArray)
      this.fDTree.removeNonFDs(distributedAttribute)//TODO: recalculate [depth] of the fdTree
      this.generateNewFDs(nonFDs.toArray)
      rmNonFds(nonFDs, fdQueue, slaverTables)
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
        for(i <- 0 until slaverTables.size){
          slaverTables(i) = new SlaverTaskTableImpl(currentLevelFDs, numAttributes, 1)
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
          for(i <- 0 until slaverTables.size){
            slaverTables(i) = new SlaverTaskTableImpl(currentLevelFDs, numAttributes, 1)
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
            for(i <- 0 until slaverTables.size){
              slaverTables(i) = new SlaverTaskTableImpl(currentLevelFDs, numAttributes, 1)
            }
          }
        }
      }
    }

    val assignment = new mutable.HashMap[Int, ArrayBuffer[FDs]]()

    while(assignment.size < numPartitions && !(fdQueue.isEmpty)){//assign task
      val curCandidateFD = fdQueue.poll()
      var slaveNum: Int = 0
      while(slaveNum < numPartitions
            && !slaverTables(slaveNum).getTasks(curCandidateFD)){
        slaveNum += 1
      }
      if(slaveNum < numPartitions){
        val lhs = curCandidateFD.getLhs.get(0, numAttributes)
        val rhss = curCandidateFD.getRhss.get(0, numAttributes)
        val curCandidateFDCopy = new FDs(lhs, rhss)
        val slaveTask = new ArrayBuffer[FDs]()
        slaveTask += curCandidateFDCopy
        assignment += (slaveNum -> slaveTask)
        curCandidateFD.increaseValidateFlag
        fdQueue.add(curCandidateFD)
      }
    }
    val newAssignment = assignment.map(f=>(f._1, f._2.toArray)).toMap
    (goToSampler, newAssignment)
  }

}
