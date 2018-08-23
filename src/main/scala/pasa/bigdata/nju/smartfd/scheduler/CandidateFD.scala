package pasa.bigdata.nju.smartfd.scheduler

import java.util

import pasa.bigdata.nju.smartfd.structures.{FDTree, FDTreeElementLhsPair, FDs}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CandidateFD{
  /**
    * get all candidate fds of current attribute
    * @param level
    * @param currentAttribute
    * @param numAttributes
    * @param fDTree
    * @return
    */
  def getAllCandidateFD(level: Int, currentAttribute: Int, numAttributes: Int, fDTree: FDTree): Array[FDs] ={
    val candidateFD = new ArrayBuffer[FDs]()
    for(i <- level until fDTree.getDepth){
      val currentLevel = fDTree.getLevel(i, currentAttribute)
      candidateFD ++= getCurrentLevelFDs(currentLevel, currentAttribute, numAttributes)
    }
    candidateFD.toArray
  }

  /**
    * get all candidate fds of current attribute
    * @param level
    * @param currentAttribute
    * @param numAttributes
    * @param fDTree
    * @return
    */
  def getAllCandidateFDWithMap(level: Int, currentAttribute: Int, numAttributes: Int, fDTree: FDTree): mutable.HashMap[util.BitSet, FDs] ={
    val candidateFD = new mutable.HashMap[util.BitSet, FDs]()
    for(i <- level until fDTree.getDepth){
      val currentLevel = fDTree.getLevel(i, currentAttribute)
      candidateFD ++= getCurrentLevelFDsWithMap(currentLevel, currentAttribute, numAttributes)
    }
    candidateFD
  }

  /**
    * Get candidate fds from nodes of level i. If there are no candidate fds of level i,
    * get nodes of level i+1 and then get candidate fds.
    */
  private def getCurrentLevelFDs(currentLevel: mutable.HashMap[util.BitSet, FDTreeElementLhsPair],
                         distributedAttribute: Int,
                         numAttributes: Int): ArrayBuffer[FDs] ={
    val candidateFD = new ArrayBuffer[FDs]()
    if(currentLevel.size != 0){//It comes to the last level when current level having no nodes.
    val currentLevelValue = currentLevel.values.toArray
      if(currentLevelValue(0).getLevel == 0){// the zaro level, only need to add the distributed attribute as the rhs, the lhs is null
        if(currentLevelValue(0).getElement.getFds.get(distributedAttribute)){
          val lhs = new util.BitSet(numAttributes)
          val rhs = new util.BitSet(numAttributes)
          rhs.set(distributedAttribute)
          candidateFD += new FDs(lhs.get(0, numAttributes), rhs.get(0, numAttributes))
        }
      }else{//other levels
        for(i <- 0 until currentLevelValue.size){
          val lhs = currentLevelValue(i).getLhs
          val rhss = currentLevelValue(i).getElement.getFds.get(0, numAttributes)
          if(rhss.cardinality() != 0){
            candidateFD += new FDs(lhs.get(0, numAttributes), rhss.get(0, numAttributes))
          }
        }
      }
    }
    candidateFD
  }

  /**
    * Get candidate fds from nodes of level i. If there are no candidate fds of level i,
    * get nodes of level i+1 and then get candidate fds.
    */
  private def getCurrentLevelFDsWithMap(currentLevel: mutable.HashMap[util.BitSet, FDTreeElementLhsPair],
                                 distributedAttribute: Int,
                                 numAttributes: Int): mutable.HashMap[util.BitSet, FDs] ={
    val candidateFD = new mutable.HashMap[util.BitSet, FDs]()
    if(currentLevel.size != 0){//It comes to the last level when current level having no nodes.
    val currentLevelValue = currentLevel.values.toArray
      if(currentLevelValue(0).getLevel == 0){// the zaro level, only need to add the distributed attribute as the rhs, the lhs is null
        if(currentLevelValue(0).getElement.getFds.get(distributedAttribute)){
          val lhs = new util.BitSet(numAttributes)
          val rhss = new util.BitSet(numAttributes)
          rhss.set(distributedAttribute)
          candidateFD += (lhs.get(0, numAttributes) ->
            new FDs(lhs.get(0, numAttributes), rhss.get(0, numAttributes)))
        }
      }else{//other levels
        for(i <- 0 until currentLevelValue.size){
          val lhs = currentLevelValue(i).getLhs
          val rhss = currentLevelValue(i).getElement.getFds.get(0, numAttributes)
          if(rhss.cardinality() != 0){
            candidateFD += (lhs.get(0, numAttributes) ->
              new FDs(lhs.get(0, numAttributes), rhss.get(0, numAttributes)))
          }
        }
      }
    }
    candidateFD
  }
}
