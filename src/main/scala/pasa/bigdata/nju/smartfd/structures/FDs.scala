package pasa.bigdata.nju.smartfd.structures

import java.util

class FDs(lhs: util.BitSet, rhss: util.BitSet) extends Serializable{// store fds and flags

  var resultFlag = -1 //"-1": not validate; "0": non-fd; "1": fd
  var validateFlag = 0//validated times in one run.

  def getLhs = this.lhs
  def getRhss = this.rhss
  def numFDs = rhss.cardinality()

  def removeFDs(removedRhss: util.BitSet): Unit ={
    rhss.andNot(removedRhss)
  }

  def getResultFlag = this.resultFlag
  def setResultFlag(flag: Int): Unit ={
    this.resultFlag = flag
  }

  def setValidateFlag(flag: Int): Unit ={
    this.validateFlag = flag
  }
  def getValidateFlag = this.validateFlag
  def increaseValidateFlag: Unit ={
    this.validateFlag += 1
  }

}
