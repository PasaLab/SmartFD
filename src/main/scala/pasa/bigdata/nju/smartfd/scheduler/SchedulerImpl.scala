package pasa.bigdata.nju.smartfd.scheduler

import pasa.bigdata.nju.smartfd.structures.FDs


abstract class SchedulerImpl extends Scheduler{
  /**
    * get all candidate fds of current attribute greater than or equal to current level
    *
    * @return
    */
  override def getAllCandidateFDs: Array[FDs] = null
}
