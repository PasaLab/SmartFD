package pasa.bigdata.nju.smartfd.structures


/**
  * store pli(for more information, please refer to HyFD for details)
  */
class PositionListIndex(val attribute: Int,
                        val clusters: Array[Array[Int]]) extends Serializable {
  
  var numNonUniqueValues: Int = countNonUniqueValuesIn(clusters)
  protected def countNonUniqueValuesIn(clusters: Array[Array[Int]]): Int = {
    var  numNonUniqueValues: Int = 0
    for(arr <- clusters){
      numNonUniqueValues += arr.size
    }
    numNonUniqueValues
  }

  def getClusters() = {
    this.clusters
  }

  def size(): Int = {
    this.clusters.size
  }

}
