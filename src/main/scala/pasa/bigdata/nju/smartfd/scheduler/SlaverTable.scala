package pasa.bigdata.nju.smartfd.scheduler

import java.util

import pasa.bigdata.nju.smartfd.structures.FDs

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait SlaverTable {

  var capacity: Int = 0

  def reInit(candidateFDs: mutable.HashMap[util.BitSet, FDs]): Unit

  /**
    * Assign task for this slaver
 *
    * @param candidateFD
    * @return If this slaver does not assign a task and needs to validate the
    *         current candidateFD, return [true] else return [false].
    */
  def getTasks(candidateFD: FDs): Boolean

  /**
    * Remove fds those are invalid or validate by this slaver from [candidateFDs].
 *
    * @param nonFDs invalid fds
    */
  def rmNonFdsFromSlaverTable(nonFDs: ArrayBuffer[FDs]): Unit

  /**
    * Determine whether the [candidateFDs] id empty.
    * @return {{true}} is there is no task
    */
  def isTaskEmpty: Boolean

  /**
    * Get the task set which will be validate in corresponding slaver
    * @return
    */
  def getValidateTasks: mutable.HashMap[util.BitSet, FDs]

  def setCapacity(capacity: Int): Unit = {
    this.capacity = capacity
  }
}
