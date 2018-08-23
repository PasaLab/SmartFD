package pasa.bigdata.nju.smartfd.inductor
import pasa.bigdata.nju.smartfd.structures.FDList

trait AbstractInductor extends InductorInterface{
  override def updatePositiveCover(nonFds: FDList): Int = 0
  override def updatePositiveCover(nonFds: FDList, currentAttribute: Int): Int = 0
}
