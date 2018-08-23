package pasa.bigdata.nju.smartfd.structures

import java.util


class FDList extends Serializable{
  private  var fdLevels: util.ArrayList[util.ArrayList[util.BitSet]] = null
  private var depth: Int = 0
  def this(numAttributes: Int) {
    this()
    this.fdLevels = new util.ArrayList[util.ArrayList[util.BitSet]](numAttributes+1)
    for(i <- 0 to numAttributes){
      this.fdLevels.add(new util.ArrayList[util.BitSet]())
    }
  }

  def getFdLevels: util.ArrayList[util.ArrayList[util.BitSet]] = {
    this.fdLevels
  }


  def getDepth: Int = this.depth

  def add(fd: util.BitSet): Boolean = {
      val length = fd.cardinality()
      this.depth = Math.max(this.depth, length)
      this.fdLevels.get(length).add(fd)
      true
  }

  def contains(fd: util.BitSet): Boolean = {
    val length: Int = fd.cardinality()
    this.fdLevels.get(length).contains(fd)
  }

  def size: Int = {
    var size: Int = 0
    for(i <- 0 until this.fdLevels.size()){
      size += this.fdLevels.get(i).size
    }
    size
  }

}
