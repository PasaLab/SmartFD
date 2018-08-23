package pasa.bigdata.nju.smartfd.sampler
import pasa.bigdata.nju.smartfd.structures.{FDList, FDs}
import pasa.bigdata.nju.smartfd.validator.ValidationResult


abstract class AbstractSampler extends Sampler {
  override def takeSample(): FDList = new FDList()

  override def takeSample1(): SampleResult = new SampleResult(new FDList(), 0l)

  //Design for integrated sampling and verification
  override def validate(fds: Array[FDs]): ValidationResult = null
}
