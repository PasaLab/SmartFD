package pasa.bigdata.nju.smartfd.structures


class ClusterIdentifierWithRecord(cluster: Array[Int],
                                  record: Int) extends ClusterIdentifier(cluster){
  def getRecord: Int = {
    return this.record
  }
}
