package pasa.bigdata.nju.smartfd.skewproducer

/**
  * Use Lhs as a key for data redistribution on skew attributes
  */
case class LhsKey(val data: Array[Int]) {
  override def toString: String = data.mkString(",")

  override def hashCode(): Int = {
    if (data == null) return 0
    var hash = 1
    var index = 0
    while(index < data.size){
      hash = 31 * hash + data(index)
      index += 1
    }
    return hash
  }

  override def equals(obj: scala.Any): Boolean = {
    if(obj == null || !obj.isInstanceOf[LhsKey]){
      return false
    }else{
      val other = obj.asInstanceOf[LhsKey]
      var index = 0
      while(index < data.length){
        if(data(index) != other.data(index)){
          return false
        }
        index += 1
      }
      return true
    }
  }

}
