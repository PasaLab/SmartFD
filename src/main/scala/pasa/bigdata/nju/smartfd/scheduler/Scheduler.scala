package pasa.bigdata.nju.smartfd.scheduler

import pasa.bigdata.nju.smartfd.structures.FDs

import scala.collection.mutable.ArrayBuffer

/**
  * An scheduler that assigns tasks for each partition. Different scheduling strategy
  * can be implemented by extends this scheduler. Now we provide four scheduling
  * strategies, which are onefd, onetime, twice and enhance.
  *
  * onefd: a partition test one candidate fd each time. If the number of partition
  * is less than the number of candidate fds, then each partition validate different
  * candidate fds, otherwise, one candidate fd will be validate in different partitions.
  *
  * onetime: a partition test all of the candidate fds each time.
  *
  * twice: first a partition test one candidate fd, and second a partition test all
  * of the remaining candidate fd exclude invalid candidate fds of the first run.
  *
  * enhance: this strategy is similarity to onefd, but a partition can validate multiple
  * candidate fds.
  */
trait Scheduler extends Serializable{

  /**
    * Get task for each partition.
    * @param nonFDs invalid functional dependencies, which are used to pure the
    *               candidate tree.
    * @return
    */
  def getTasks(nonFDs: ArrayBuffer[FDs])
  : (Boolean, Map[Int, Array[FDs]])

  /**
    * get task after detection
    * @param nonFDs
    * @return
    */
  def reGetTasks(nonFDs: ArrayBuffer[FDs])
  : (Boolean, Map[Int, Array[FDs]])

  /**
    * get all candidate fds of current attribute greater than or equal to current level
    * @return
    */
  def getAllCandidateFDs: Array[FDs]
}
