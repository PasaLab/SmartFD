package pasa.bigdata.nju.smartfd.scheduler.onetime

import pasa.bigdata.nju.smartfd.scheduler.{CandidateFD, TaskTableImpl}
import pasa.bigdata.nju.smartfd.structures.{FDTree, FDs}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class OneTimeTaskTable(fDTree: FDTree,
                       numPartitions: Int,
                       distributedAttribute: Int,
                       numAttributes: Int,
                       efficiencyThreshold: Float)
  extends TaskTableImpl(fDTree, distributedAttribute, numAttributes) {

  private var numOfFDs: Int = 0
  private var numOfInvalidFDs: Int = 0
  private var goToSampler: Boolean = false

  override def getAllCandidateFDs: Array[FDs] = {
    val level = if(this.level - 1 >= 0) this.level - 1 else 0
    CandidateFD.getAllCandidateFD(level, distributedAttribute, numAttributes, fDTree)
  }

  def reGetTasks(nonFDs: ArrayBuffer[FDs]): (Boolean, Map[Int, Array[FDs]]) = {

    if(currentLevelFDs != null) currentLevelFDs.clear()
    assert(this.level != 0, "In OneTimeTaskTable, wrong level zero!!!")
    this.level -= 1
    rmNonFDsFromCurrentLevel(nonFDs.toArray)
    generateNewFDs(nonFDs.toArray)
    this.fDTree.removeNonFDs(distributedAttribute)//TODO: recalculate [depth] of the fdTree

    currentLevel = this.fDTree.getLevel(this.level, this.distributedAttribute)
    this.level += 1
    this.getCurrentLevelFDs
    val assignment = new mutable.HashMap[Int, Array[FDs]]()
    if(currentLevelFDs.size > 0){
      val slaveTask = new ArrayBuffer[FDs]()
      for((_, fds) <- currentLevelFDs){
        val lhs = fds.getLhs.get(0, numAttributes)
        val rhss = fds.getRhss.get(0, numAttributes)
        slaveTask.+=(new FDs(lhs, rhss))
      }
      for(i <- 0 until numPartitions){
        assignment += (i -> slaveTask.toArray)
      }
    }
    (goToSampler, assignment.toMap)
  }


  override def getTasks(nonFDs: ArrayBuffer[FDs]): (Boolean, Map[Int, Array[FDs]]) = {
    if(currentLevelFDs != null) currentLevelFDs.clear()
    if(this.level == 0){//first calling. Need to get the next level fds
      this.getCurrentLevel
      this.getCurrentLevelFDs
    }else{//current level fds' validation has been finished. Need to do some cleaning job and get the next level fds
      if(goToSampler){//if jump to [Sampler] last time, need to get currentLevel from the root of the fdTree(Becase the fdTree has been modified).
        this.goToSampler = false
        currentLevel = this.fDTree.getLevel(this.level, this.distributedAttribute)
        this.level += 1
        this.getCurrentLevelFDs
      }else{
        rmNonFDsFromCurrentLevel(nonFDs.toArray)
        generateNewFDs(nonFDs.toArray)
        this.fDTree.removeNonFDs(distributedAttribute)//TODO: recalculate [depth] of the fdTree
        numOfInvalidFDs = nonFDs.map(f=>f.getRhss.cardinality()).sum
        if(!validationIsEfficiency1(numOfInvalidFDs, numOfFDs, efficiencyThreshold)){// if validation efficiency becomes low, go to sampler
          println("in task table， go to sample")
          goToSampler = true
          numOfFDs = 0
          numOfInvalidFDs = 0
        }else{
          currentLevel = this.fDTree.getLevel(this.level, this.distributedAttribute)
          this.level += 1
          this.getCurrentLevelFDs
        }
      }
    }

    numOfFDs = currentLevelFDs.map(f=>f._2.getRhss.cardinality()).sum
    val assignment = new mutable.HashMap[Int, Array[FDs]]()

    if(currentLevelFDs.size > 0){
      val slaveTask = new ArrayBuffer[FDs]()
      for((_, fds) <- currentLevelFDs){
        val lhs = fds.getLhs.get(0, numAttributes)
        val rhss = fds.getRhss.get(0, numAttributes)
        slaveTask.+=(new FDs(lhs, rhss))
      }

      for(i <- 0 until numPartitions){
        assignment += (i -> slaveTask.toArray)
      }
    }
    (goToSampler, assignment.toMap)
  }

}
