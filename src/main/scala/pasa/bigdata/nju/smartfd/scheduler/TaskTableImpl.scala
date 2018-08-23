package pasa.bigdata.nju.smartfd.scheduler

import java.util
import java.util.{Comparator, PriorityQueue}

import pasa.bigdata.nju.smartfd.structures.{FDTree, FDTreeElementLhsPair, FDs}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

abstract class TaskTableImpl(val fDTree: FDTree,
                             val distributedAttribute: Int,
                             val numAttributes: Int) extends TaskTable {
  var currentLevel: mutable.HashMap[util.BitSet, FDTreeElementLhsPair] = null
  var level: Int = 0
  var currentLevelFDs = new mutable.HashMap[util.BitSet, FDs]()
  val totalNonFDs = new mutable.HashMap[util.BitSet, FDs]()

  override def getTasks(nonFDs: ArrayBuffer[FDs]): (Boolean, Map[Int, Array[FDs]])

  override def getAllCandidateFDs: Array[FDs] = null
  /**
    * Get candidate fds from nodes of level i. If there are no candidate fds of level i,
    * get nodes of level i+1 and then get candidate fds.
    */
  def getCurrentLevelFDs: Unit ={
    if(currentLevel.size != 0){//It comes to the last level when current level having no nodes.
    val currentLevelValue = currentLevel.values.toArray
      if(currentLevelValue(0).getLevel == 0){// the zaro level, only need to add the distributed attribute as the rhs, the lhs is null
        if(currentLevelValue(0).getElement.getFds.get(distributedAttribute)){
          val lhs = new util.BitSet(numAttributes)
          val rhs = new util.BitSet(numAttributes)
          rhs.set(distributedAttribute)
          currentLevelFDs += (lhs.get(0, numAttributes) ->
            new FDs(lhs.get(0, numAttributes), rhs.get(0, numAttributes)))
        }else if(this.level <= fDTree.getDepth){
          this.getCurrentLevel
          this.getCurrentLevelFDs
        }
      }else{//other levels
        for(i <- 0 until currentLevelValue.size){
          val lhs = currentLevelValue(i).getLhs
          val rhss = currentLevelValue(i).getElement.getFds.get(0, numAttributes)
          if(rhss.cardinality() != 0){
            currentLevelFDs += (lhs.get(0, numAttributes) ->
              new FDs(lhs.get(0, numAttributes), rhss.get(0, numAttributes)))
          }
        }
        if(this.currentLevelFDs.size == 0 && this.level <= fDTree.getDepth){
          this.getCurrentLevel
          this.getCurrentLevelFDs
        }
      }
    }
  }

  /**
    * Get candidate fds from nodes of level i. If there are no candidate fds of level i,
    * get nodes of level i+1 and then get candidate fds.
    */
  def  getCurrentLevelFDsAndRemoveValidateFD(otherLevelCandidataFDs: mutable.HashMap[util.BitSet, FDs]): Unit ={
    if(currentLevel.size != 0){//It comes to the last level when current level having no nodes.
      val currentLevelValue = currentLevel.values.toArray
      if(currentLevelValue(0).getLevel == 0){// the zaro level, only need to add the distributed attribute as the rhs, the lhs is null
        if(currentLevelValue(0).getElement.getFds.get(distributedAttribute)){
          val lhs = new util.BitSet(numAttributes)
          val rhs = new util.BitSet(numAttributes)
          rhs.set(distributedAttribute)
          currentLevelFDs += (lhs.get(0, numAttributes) ->
            new FDs(lhs.get(0, numAttributes), rhs.get(0, numAttributes)))
        }else if(this.level <= fDTree.getDepth){
          this.getCurrentLevel
          this.getCurrentLevelFDsAndRemoveValidateFD(otherLevelCandidataFDs)
        }
      }else{//other levels
        for(i <- 0 until currentLevelValue.size){
          val lhs = currentLevelValue(i).getLhs
          val rhss = currentLevelValue(i).getElement.getFds.get(0, numAttributes)
          if(otherLevelCandidataFDs.contains(lhs)) rhss.andNot(otherLevelCandidataFDs(lhs).getRhss)
          if(rhss.cardinality() != 0){
            currentLevelFDs += (lhs.get(0, numAttributes) ->
              new FDs(lhs.get(0, numAttributes), rhss.get(0, numAttributes)))
          }
        }
        if(this.currentLevelFDs.size == 0 && this.level <= fDTree.getDepth){
          this.getCurrentLevel
          this.getCurrentLevelFDsAndRemoveValidateFD(otherLevelCandidataFDs)
        }
      }
    }
  }

  /**
    * Get nodes of [this.level]
    */
  def getCurrentLevel:Unit = {
    if(this.level == 0){// the zero level, add the root to current level
      currentLevel = new mutable.HashMap[util.BitSet, FDTreeElementLhsPair]()
      val lhs = new util.BitSet(numAttributes)
      val fDTreeElementLhsPair = new FDTreeElementLhsPair(fDTree, new util.BitSet(numAttributes), this.level)
      currentLevel += (lhs -> fDTreeElementLhsPair)
      this.level += 1
    }else if(this.level == 1){// the first level, add the distributed attribute to the current level
      currentLevel = new mutable.HashMap[util.BitSet, FDTreeElementLhsPair]()
      if(fDTree.getChildren != null && fDTree.getChildren(distributedAttribute) != null){
        val lhs = new util.BitSet(numAttributes)
        lhs.set(distributedAttribute)
        val fDTreeElementLhsPair = new FDTreeElementLhsPair(fDTree.getChildren(distributedAttribute), lhs.get(0, numAttributes), this.level)
        currentLevel += (lhs -> fDTreeElementLhsPair)
      }
      this.level += 1
    }else{// other levels, collect children of the current level
      val nextLevel = new mutable.HashMap[util.BitSet, FDTreeElementLhsPair]()
      val currentLevelValue = currentLevel.values.toArray
      for(i <-0 until currentLevelValue.size){
        val children = currentLevelValue(i).getElement.getChildren
        val lhs = currentLevelValue(i).getLhs
        if(children != null){
          for(j <- 0 until children.size){
            if(children(j) != null){
              lhs.set(j)
              nextLevel +=(lhs.get(0, numAttributes) ->
                new FDTreeElementLhsPair(children(j), lhs.get(0, numAttributes), this.level))
              lhs.clear(j)
            }
          }
        }
      }
      currentLevel = nextLevel
      this.level += 1
    }
  }

  /**
    * Remove invalid fds from current level nodes of the fDTree
    */
  def rmNonFDsFromCurrentLevel(nonFDs: Array[FDs]): Unit ={
    for(nonFD <- nonFDs){
      if(currentLevel.contains(nonFD.getLhs)){
        val currentLevelFD = currentLevel(nonFD.getLhs)
        currentLevelFD.getElement.getFds.andNot(nonFD.getRhss)
      }
    }
  }

  /**
    * Generate new candidate fds of the next level from invalid fds
    *
    * @param nonFDs invalid fds
    */
  def generateNewFDs(nonFDs: Array[FDs]): Unit = {
    for(fds <- nonFDs){
      val lhs = fds.getLhs
      val rhss = fds.getRhss
      var rhs = rhss.nextSetBit(0)
      while(rhs >= 0 && rhs < numAttributes){
        // Because of the sorting of attributes, we validate the lhs which with higher cardinality first.
        // So, if B -> C is invalid FD, AB -> C which has been validated before needs not to be added to the fdTree.
        // Then only the attribute with lower cardinality should be added to the lhs.
        // Because AB -> C is validate before B -> C, if B -> C id valid, AB -> C should be removed.
        for(extensionAttr <-(distributedAttribute + 1 until numAttributes).reverse){
          val childLhs = this.extendWith(lhs, rhs, extensionAttr)
          if(childLhs != null) {
            this.fDTree.addFunctionalDependencyGetIfNew(childLhs, rhs)
          }
        }
        rhs = rhss.nextSetBit(rhs+1)
      }
    }
  }

  /**
    * Determine whether lhs -> rhs and lhs.set(extensionAttr) should be generated.(pruning rule)
    *
    * @param lhs
    * @param rhs
    * @param extensionAttr
    * @return
    */
  private def extendWith(lhs: util.BitSet, rhs: Int, extensionAttr: Int): util.BitSet ={
    var nextLhsAttr = lhs.nextSetBit(0)
    if(lhs.get(extensionAttr) ||             // Triviality: AA->C cannot be valid, because A->C is invalid
      (rhs == extensionAttr) ||              // Triviality: AC->C cannot be valid, because A->C is invalid
      ((this.fDTree.getChildren != null) &&
        (this.fDTree.getChildren(distributedAttribute) != null) &&
        (this.fDTree.getChildren(distributedAttribute).containsFdOrGeneralization(lhs, extensionAttr, nextLhsAttr))))
      return null

    val childLhs: util.BitSet = lhs.get(0, numAttributes)
    childLhs.set(extensionAttr)
    nextLhsAttr = childLhs.nextSetBit(0)
    if((this.fDTree.getChildren != null) &&
      (this.fDTree.getChildren(distributedAttribute) != null) &&
      this.fDTree.getChildren(distributedAttribute).containsFdOrGeneralization(childLhs, rhs, nextLhsAttr))
      return null

    return childLhs
  }

  /**
    * Remove invalid fds from the driver task table and each slaver task table.
    * Remove fds from each slaver task table.
    *
    * @param nonFDs invalid fds
    */
  def rmNonFds(nonFDs: ArrayBuffer[FDs],
               fdQueue: PriorityQueue[FDs],
               slaverTable: Array[SlaverTable]): Unit ={//after each round of validation, we need to remove nonFDs

    rmNonFdFromTaskTable(nonFDs, fdQueue)
    var numOfSlaverWithTask = 0
    for(slaveTask <- slaverTable){
      slaveTask.rmNonFdsFromSlaverTable(nonFDs)
      if(!slaveTask.isTaskEmpty){
        numOfSlaverWithTask += 1
      }
    }
    if(numOfSlaverWithTask == 0){
      fdQueue.clear()
      currentLevelFDs.clear()
    }
  }

  /**
    * Remove invalid fds from candidate fds and generate new fdQueue
    *
    * @param nonFDs invalid fds
    */
  private def rmNonFdFromTaskTable(nonFDs: ArrayBuffer[FDs],
                           fdQueue: PriorityQueue[FDs]): Unit ={
    for(nonFD <- nonFDs){
      val lhs = nonFD.getLhs
      val rhss = nonFD.getRhss
      if(currentLevelFDs.contains(lhs)){
        val candidateFD = currentLevelFDs(lhs)
        candidateFD.getRhss.andNot(rhss)
        if(candidateFD.getRhss.cardinality() == 0){
          currentLevelFDs.remove(lhs)
        }
      }
    }

    fdQueue.clear()
    for((_, candidateFD) <- currentLevelFDs){
      val lhs = candidateFD.getLhs.get(0, numAttributes)
      val rhss = candidateFD.getRhss.get(0, numAttributes)
      fdQueue.add(new FDs(lhs, rhss))
    }
  }

  /**
    * calculate validation efficiency
    *
    * @param numInvalidFDs
    * @param numValidateFDs
    * @param efficiencyThreshold
    * @return If validation is efficiency, return true; else, return false.
    */
  def validationIsEfficiency(numInvalidFDs: Int,
                             numValidateFDs: Int,
                             efficiencyThreshold: Float): Boolean = {
    if((numInvalidFDs.toDouble / (numValidateFDs + numInvalidFDs) > efficiencyThreshold) && numInvalidFDs > 0){
      false
    }else{
      true
    }
  }

  def validationIsEfficiency1(numInvalidFDs: Int,
                              totalFDs: Int,
                             efficiencyThreshold: Float): Boolean = {
    if((numInvalidFDs.toDouble / totalFDs > efficiencyThreshold) && numInvalidFDs > 0){
      println("invalidate efficiency: " + numInvalidFDs.toDouble / totalFDs)
      false
    }else{
      true
    }
  }

  /**
    * Comparator for fdQueue
    */
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
