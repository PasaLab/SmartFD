package pasa.bigdata.nju.smartfd.sampler

import pasa.bigdata.nju.smartfd.structures.{FDList, FDs}
import pasa.bigdata.nju.smartfd.validator.ValidationResult

trait Sampler {
  def takeSample(): FDList
  def takeSample1(): SampleResult

  //validate using sample data structure
  def validate(fds: Array[FDs]): ValidationResult
}
