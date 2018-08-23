package pasa.bigdata.nju.smartfd.validator

import java.util

import pasa.bigdata.nju.smartfd.structures.{FDs, IntegerPair}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ValidationResult extends Serializable{
  def this(partitionID: Int, numAttribute: Int){
    this
    this.partitionID = partitionID
    comparisonSuggestions = new mutable.HashMap[Int,util.ArrayList[IntegerPair]]
    comparisonSuggestions += (partitionID -> new util.ArrayList[IntegerPair]())
    this.numAttribute = numAttribute
  }
  var partitionID = 0
  var validations: Int = 0
  var intersections: Int = 0
  var invalidFDs = new mutable.HashMap[util.BitSet, util.BitSet]()
  var comparisonSuggestions: mutable.HashMap[Int, util.ArrayList[IntegerPair]] = new mutable.HashMap[Int, util.ArrayList[IntegerPair]]()
  val fdSet: util.HashSet[util.BitSet] = new util.HashSet[util.BitSet]()
  var numAttribute: Int = 0
  var validatorTime: Long = 0l

  def add(other: ValidationResult) {
    this.validations += other.validations
    this.intersections += other.intersections
    addInvalidFDs(other.invalidFDs)
    fdSet.addAll(other.fdSet)
    comparisonSuggestions += (other.partitionID -> other.comparisonSuggestions(other.partitionID))
  }

  def addResult(other: ValidationResult):ValidationResult = {
    this.validations += other.validations
    this.intersections += other.intersections
    addInvalidFDs(other.invalidFDs)
    fdSet.addAll(other.fdSet)
    return this
  }

  def addFDSet(fdLevels: Array[mutable.HashSet[util.BitSet]]): Unit ={
    for(i <-0 until fdLevels.length){
      val curLevel = fdLevels(i)
      for(bitSet <- curLevel){
        println(bitSet)
        this.fdSet.add(bitSet)
      }
    }
  }

  def addInvalidFDs(other: mutable.HashMap[util.BitSet, util.BitSet]): Unit = {
    for(invalidFD <- other){
      if(invalidFDs.contains(invalidFD._1)){
        invalidFDs(invalidFD._1).or(invalidFD._2)
      }else{
        invalidFDs += (invalidFD._1 -> invalidFD._2)
      }
    }
  }

  def getNonFDs: util.ArrayList[FDs] = {
    val nonFDs = new util.ArrayList[FDs]()
    for(invalidFD <- invalidFDs){
      nonFDs.add(new FDs(invalidFD._1, invalidFD._2))
    }
    nonFDs
  }

  def getNonFDsToBuffer: ArrayBuffer[FDs] = {
    val nonFDs = new ArrayBuffer[FDs]()
    for(invalidFD <- invalidFDs){
      nonFDs += (new FDs(invalidFD._1, invalidFD._2))
    }
    nonFDs
  }

  def addInvalidFDs(lhs: util.BitSet, rhs: util.BitSet): Unit = {
    if(invalidFDs.contains(lhs)){
      invalidFDs(lhs).or(rhs)
    }else{
      invalidFDs += (lhs -> rhs)
    }
  }

}
