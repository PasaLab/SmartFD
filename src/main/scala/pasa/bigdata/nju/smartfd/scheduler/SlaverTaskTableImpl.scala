package pasa.bigdata.nju.smartfd.scheduler

import java.util

import pasa.bigdata.nju.smartfd.structures.FDs

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class SlaverTaskTableImpl extends SlaverTable {
  private var flag = false//false: before assignment; true: after assignment
  private var candidateFDs: mutable.HashMap[util.BitSet, FDs] = null
  private var numAttributes: Int = 0
  private var currentTaskSize = 0

  def this(candidateFDs: mutable.HashMap[util.BitSet, FDs],
           numAttributes: Int,
           capacity: Int){
    this
    this.capacity = capacity
    this.numAttributes = numAttributes
    this.candidateFDs = new mutable.HashMap[util.BitSet, FDs]()
    for((k, v) <- candidateFDs){
      val lhs = v.getLhs
      val rhss = v.getRhss
      val thisFD = new FDs(lhs.get(0, numAttributes), rhss.get(0, numAttributes))
      this.candidateFDs += (k.get(0, numAttributes) -> thisFD)
    }
  }

  def this(candidateFDs: Array[FDs],
           numAttributes: Int,
           capacity: Int){
    this
    this.capacity = capacity
    this.numAttributes = numAttributes
    this.candidateFDs = new mutable.HashMap[util.BitSet, FDs]()
    for(fd <- candidateFDs){
      val lhs = fd.getLhs
      val rhss = fd.getRhss
      val thisFD = new FDs(lhs.get(0, numAttributes), rhss.get(0, numAttributes))
      this.candidateFDs += (lhs.get(0, numAttributes) -> thisFD)
    }
  }

  override def reInit(candidateFDs: mutable.HashMap[util.BitSet, FDs]): Unit ={
    this.flag = false
    this.currentTaskSize = 0

    for((k, v) <- candidateFDs){
      val lhs = v.getLhs
      val rhss = v.getRhss
      val thisFD = new FDs(lhs.get(0, numAttributes), rhss.get(0, numAttributes))
      this.candidateFDs += (k.get(0, numAttributes) -> thisFD)
    }
  }

  /**
    * Assign task for this slaver
    *
    * @param candidateFD
    * @return If this slaver does not assign a task and needs to validate the
    *         current candidateFD, return [true] else return [false].
    */
  override def getTasks(candidateFD: FDs): Boolean ={

    if(currentTaskSize >= capacity){
      false
    }else{
      val lhs = candidateFD.getLhs
      if(candidateFDs.contains(lhs)){
        currentTaskSize += 1
        candidateFDs.remove(lhs)
        true
      }else{
        false
      }
    }
  }

  /**
    * Remove fds those are invalid or validate by this slaver from [candidateFDs].
    *
    * @param nonFDs invalid fds
    */
  override def rmNonFdsFromSlaverTable(nonFDs: ArrayBuffer[FDs]): Unit ={
    for(nonFD <- nonFDs){
      val lhs = nonFD.getLhs
      val rhss = nonFD.getRhss
      if(candidateFDs.contains(lhs)){
        val candidateFD = candidateFDs(lhs)
        candidateFD.getRhss.andNot(rhss)
        if(candidateFD.getRhss.cardinality() == 0){
          candidateFDs.remove(lhs)
        }
      }
    }

    if(candidateFDs.size < capacity){
      capacity = candidateFDs.size
    }
    currentTaskSize = 0
  }

  /**
    * Determine whether the [candidateFDs] id empty.
    *
    * @return
    */
  override def isTaskEmpty: Boolean = {
    if(this.candidateFDs.isEmpty){
      true
    }else{
      false
    }
  }

  override def getValidateTasks: mutable.HashMap[util.BitSet, FDs] = candidateFDs
}
