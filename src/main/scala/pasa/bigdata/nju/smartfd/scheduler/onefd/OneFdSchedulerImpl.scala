package pasa.bigdata.nju.smartfd.scheduler.onefd

import pasa.bigdata.nju.smartfd.conf.Conf
import pasa.bigdata.nju.smartfd.scheduler.{SchedulerImpl, TaskTable}
import pasa.bigdata.nju.smartfd.structures.{FDTree, FDs}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class OneFdSchedulerImpl extends SchedulerImpl {
  var taskTable: TaskTable = null
  var partitionWithRecord: IndexedSeq[Int] = null
  def this(fDTree: FDTree,
           conf: Conf,
           distributeAttribute: Int,
           numRecordsOfPartition: Array[Int]){
    this
    this.partitionWithRecord =
      for(partition <- 0 until numRecordsOfPartition.size if numRecordsOfPartition(partition) != 0)
        yield partition
    this.taskTable = new OneFDTaskTable(fDTree,
      partitionWithRecord.size,
      distributeAttribute,
      conf.numAttributes,
      conf.validatorThreshold)
  }
  override def getTasks(nonFDs: ArrayBuffer[FDs]) = {
    val (goToSampler, assignment) = taskTable.getTasks(nonFDs)
    val newAssignment = new mutable.HashMap[Int, Array[FDs]]()
    for((partition, fds) <- assignment){
      newAssignment += (partitionWithRecord(partition) -> fds)
    }
    (goToSampler, newAssignment.toMap)
  }

  override def reGetTasks(nonFDs: ArrayBuffer[FDs]): (Boolean, Map[Int, Array[FDs]]) = (false, null)
}
