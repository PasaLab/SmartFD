package pasa.bigdata.nju.smartfd.scheduler.minibatch

import pasa.bigdata.nju.smartfd.scheduler.TaskTableImpl
import pasa.bigdata.nju.smartfd.structures.{FDTree, FDs}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MiniBatchTaskTable(fDTree: FDTree,
                         numPartitions: Int,
                         distributedAttribute: Int,
                         numAttributes: Int,
                         efficiencyThreshold: Float,
                         adaptiveSchedulerThreshold: Int,
                         batchSize: Int)
  extends TaskTableImpl(fDTree, distributedAttribute, numAttributes) {

  //a high priority assigned to FD with fewer validation times
  private var totalTasks: ArrayBuffer[FDs] = new ArrayBuffer[FDs]()
  private var goToSampler: Boolean = false
  private var numOfFDs: Int = 0
  private var numOfInvalidFDs: Int = 0
  private var taskStartIndex: Int = 0

  def reGetTasks(nonFDs: ArrayBuffer[FDs]): (Boolean, Map[Int, Array[FDs]]) = {
    if(currentLevelFDs.size > 0) currentLevelFDs.clear()
    if(totalTasks.size > 0) totalTasks.clear()
    taskStartIndex = 0
    assert(this.level != 0, "In Mini Batch Task Table, wrong level zero!!!")
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
      totalTasks += (new FDs(lhs, rhss))
      numOfFDs += fds.getRhss.cardinality()
    }
    val assignment = new mutable.HashMap[Int, Array[FDs]]()
    if(totalTasks.size > batchSize){
      val currentTask = totalTasks.slice(0, batchSize)
      taskStartIndex = batchSize
      assignment += (0 -> currentTask.toArray)
    }else if(totalTasks.size > 0){
      assignment += (0 -> totalTasks.toArray)
      taskStartIndex = totalTasks.size
    }
    (goToSampler, assignment.toMap)
  }

  override def getTasks(nonFDs: ArrayBuffer[FDs]) = {
    assert(nonFDs != null, "nonFDs must be initialized")
    for(nonFD <- nonFDs){
      numOfInvalidFDs += nonFD.getRhss.cardinality()
    }

    if(this.level != 0){
      this.rmNonFDsFromCurrentLevel(nonFDs.toArray)
      this.generateNewFDs(nonFDs.toArray)
      this.fDTree.removeNonFDs(distributedAttribute)//TODO: recalculate [depth] of the fdTree
    }

    //get candidate fds
    if(taskStartIndex == totalTasks.size){//maybe this is the first time calling [getTasks] or the current level fds' validation has been finished
      taskStartIndex = 0
      totalTasks.clear()
      if(this.level == 0){//first calling. Need to get the next level fds
        this.getCurrentLevel
        this.getCurrentLevelFDs
        for((_,fds) <- currentLevelFDs){
          val lhs = fds.getLhs.get(0, numAttributes)
          val rhss = fds.getRhss.get(0, numAttributes)
          totalTasks += (new FDs(lhs, rhss))
          numOfFDs += fds.getRhss.cardinality()
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
            totalTasks += (new FDs(lhs, rhss))
            numOfFDs += fds.getRhss.cardinality()
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
              totalTasks += (new FDs(lhs, rhss))
              numOfFDs += fds.getRhss.cardinality()
            }
          }
        }
      }
    }

    val assignment = new mutable.HashMap[Int, Array[FDs]]()
    if(totalTasks.size > 0){
      if(totalTasks.size - taskStartIndex > batchSize){
        assignment += (0 -> totalTasks.slice(taskStartIndex, taskStartIndex + batchSize).toArray)
        taskStartIndex += batchSize
      }else {
        assignment += (0 -> totalTasks.slice(taskStartIndex, totalTasks.size).toArray)
        taskStartIndex = totalTasks.size
      }
    }
    (goToSampler, assignment.toMap)
  }

}
