package pasa.bigdata.nju.smartfd.inductor

import pasa.bigdata.nju.smartfd.structures.FDList

trait InductorInterface {
  def updatePositiveCover(nonFds: FDList): Int
  def updatePositiveCover(nonFds: FDList, currentAttribute: Int): Int
}
