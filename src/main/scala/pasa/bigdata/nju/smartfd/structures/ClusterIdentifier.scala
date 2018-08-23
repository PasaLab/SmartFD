package pasa.bigdata.nju.smartfd.structures


class ClusterIdentifier(var cluster: Array[Int]) {

  def set(index: Int, clusterId: Int) {
    this.cluster(index) = clusterId
  }

  def get(index: Int): Int = {
    return this.cluster(index)
  }

  def getCluster: Array[Int] = {
    return this.cluster
  }

  def size: Int = {
    return this.cluster.length
  }

  override def hashCode: Int = {
    var hash: Int = 1
    var index: Int = this.size
    while (index != 0){
      hash = 31 * hash + this.get(index - 1)
      index -= 1
    }
    return hash
  }

  override def equals(any: Any): Boolean = {//does not be called
  if (!(any.isInstanceOf[ClusterIdentifier])) return false
  val other: ClusterIdentifier = any.asInstanceOf[ClusterIdentifier]
  var index: Int = this.size
  if (index != other.size) return false
  val cluster1: Array[Int] = this.getCluster
  val cluster2: Array[Int] = other.getCluster
  while (index != 0){
    if (cluster1(index -1) != cluster2(index - 1)) return false
    index -= 1
  }
  return true
}

  override def toString: String = {
    val builder: StringBuilder = new StringBuilder
    builder.append("[")
    for(i <- 0 until this.size){
      builder.append(this.cluster(i))
      if (i + 1 < this.size) builder.append(", ")
    }
    builder.append("]")
    return builder.toString
  }
}
