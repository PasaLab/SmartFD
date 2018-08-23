package pasa.bigdata.nju.smartfd.scheduler

import pasa.bigdata.nju.smartfd.structures.FDs

import scala.collection.mutable.ArrayBuffer

trait TaskTable extends Serializable{
  def getTasks(nonFDs: ArrayBuffer[FDs]): (Boolean, Map[Int, Array[FDs]])

  def reGetTasks(nonFDs: ArrayBuffer[FDs]): (Boolean, Map[Int, Array[FDs]])

  def getAllCandidateFDs: Array[FDs]
}
