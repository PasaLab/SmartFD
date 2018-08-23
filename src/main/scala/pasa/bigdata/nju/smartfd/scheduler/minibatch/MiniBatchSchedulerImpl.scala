package pasa.bigdata.nju.smartfd.scheduler.minibatch

import pasa.bigdata.nju.smartfd.conf.Conf
import pasa.bigdata.nju.smartfd.scheduler.{SchedulerImpl, TaskTable}
import pasa.bigdata.nju.smartfd.structures.{FDTree, FDs}

import scala.collection.mutable.ArrayBuffer

class MiniBatchSchedulerImpl  extends SchedulerImpl {
  var taskTable: TaskTable = null
  def this(fDTree: FDTree,
           conf: Conf,
           distributeAttribute: Int){
    this
    this.taskTable = new MiniBatchTaskTable(fDTree,
      conf.numPartition,
      distributeAttribute,
      conf.numAttributes,
      conf.validatorThreshold,
      conf.adaptiveSchedulerThreshold,
      conf.miniBatchSize)
  }
  override def getTasks(nonFDs: ArrayBuffer[FDs]) = {
    taskTable.getTasks(nonFDs)
  }

  override def reGetTasks(nonFDs: ArrayBuffer[FDs]): (Boolean, Map[Int, Array[FDs]]) = {
    taskTable.reGetTasks(nonFDs)
  }
}
