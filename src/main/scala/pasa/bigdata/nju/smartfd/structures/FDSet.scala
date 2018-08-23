package pasa.bigdata.nju.smartfd.structures


import java.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * A data structure used to store and search BitSet
  */
class FDSet extends Serializable{
  private var fdLevels: Array[mutable.HashSet[util.BitSet]] = null
  private var depth: Int = 0
  def this(numAttributes: Int){
    this()
    this.fdLevels = new Array[mutable.HashSet[util.BitSet]](numAttributes + 1)
    for(i <- 0 until numAttributes + 1){
      this.fdLevels(i) = new mutable.HashSet[util.BitSet]()
    }
  }

  def getFdLevels: Array[mutable.HashSet[util.BitSet]] = this.fdLevels

  def getDepth: Int = this.depth

  def add(fd: util.BitSet): Boolean = {
    val length: Int = fd.cardinality()
      this.depth = Math.max(this.depth, length)
      this.fdLevels(length).+=(fd)
      true
  }

  def contains(fd: util.BitSet): Boolean = {
    val length:Int = fd.cardinality()
    this.fdLevels(length).contains(fd)
  }

  def size: Int = {
    var size: Int = 0
    for(i <- 0 until this.fdLevels.length){
      size += this.fdLevels(i).size
    }
    size
  }

  def addAllNonFDs(nonFDs: Array[util.ArrayList[ArrayBuffer[util.BitSet]]]):Unit = {
    for(nonFDOfEachPartition <- nonFDs){
      for(i<- 0 until nonFDOfEachPartition.size()){
        val nonFDOfSizeI = nonFDOfEachPartition.get(i).toArray
        for(bitSet <- nonFDOfSizeI){
          if(!contains(bitSet)){
            add(bitSet)
          }
        }
      }
    }
  }

}
