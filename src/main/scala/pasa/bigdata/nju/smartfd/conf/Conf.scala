package pasa.bigdata.nju.smartfd.conf

/**
  * The configuration of SmartFD
  * example:--inputFilePath ${fileName} --numAttributes ${column} --numPartition $numPartition --inputFilePartition $inputFilePartition --outputFilePath ${fileName}_results --inputFileSeparator , --nullEqualsNull true --inputFileHasHeader false --appName QQYY${main}${fileName} --inputFromHBase false --efficiencyThreshold 0.01 --validatorThreshold 0.01 --useRearrangement true --rearrangeUsingRange true --scheduleMod onetime --rearrangeParam fre_var_skew --useSampler true --useDetection true --detectedThreshold 40 --detectedToSampleThreshold 0.1 --numOfThreadUsedInValidator 8 --useMultiThreadValidator true --useMultiThreadSampler true --numOfThreadUsedInSampler 8 --skewDataSize 20000000 --skewThreshold 0.33
  */
case class Conf(){

  var inputFilePath: String = ""
  var outputFilePath: String = ""
  var tempFilePath: String = ""

  var inputFileSeparator: String = ","
  var nullEqualsNull: Boolean = true
  var inputFileHasHeader: Boolean = false
  var numAttributes: Int = 10

  var appName: String = "SmartFD"
  var inputFilePartition: Int = 168// partition number of input file
  var numPartition: Int = 56 // partition number used in calculate

  var inputFromHBase: Boolean = false

  //if sample rate is lower than this param ,go th sample
  var efficiencyThreshold: Float = 0.01f
  //if invalid rate is larger than this param, go to sampler
  var validatorThreshold: Float = 0.2f
  //Is the property rearranged
  var useRearrangement: Boolean = true
  //Different schedule strategy, such as {{onefd, onetime, twice, multi, adaptive, minibatch}}
  var scheduleMod: String = "onetime"
  //mini batch size for minibatch scheduler mod. Works only if the schedule mode is "minibatch".
  var miniBatchSize: Int = 100000
  // use sampler or not
  var useSampler: Boolean = true
  //Different rearrange strategy, such as {{variance, frequent, var_fre, fre_var, fre_var_skew}}
  var rearrangeParam: String = "fre_var"
  //Using RangePartitioner in rearrangement
  var rearrangeUsingRange = true

  //use detection before validation
  var useDetection = true
  //using detection when the number of candidate fd is larger than this detectedThreshold
  var detectedThreshold = 30
  //if num_of_invalid_fd/num_of_fd > detectedToSampleThreshold, go to sample
  var detectedToSampleThreshold = 0.2

  //using multi scheduler when the fd number is larger than this param in adaptive scheduler
  var adaptiveSchedulerThreshold = 200

  var useMultiThreadValidator = false

  //used for multi thread validator
  var numOfThreadUsedInValidator = 8

  var useMultiThreadSampler = false

  //used for multi thread sampler sampler
  var numOfThreadUsedInSampler = 8

  var skewThreshold = 0.33

  var skewDataSize = 20000000

  //use algorithm parallelism when attribute number is equal to or larger than "algorithmParallelThreshold"
  var algorithmParallelThreshold = 0.8


  //write log or not
  var writerLocalLogs = false

  var localLogDir = ""


  override def toString() = {
    "Config:\r\n\t" +
      "inputFilePath: " + this.inputFilePath + "\r\n\t" +
      "outputFilePath: " + this.outputFilePath +"\r\n\t" +
      "inputFileSeparator: " + this.inputFileSeparator + "\r\n\t" +
      "nullEqualsNull: " + this.nullEqualsNull + "\r\n\t" +
      "inputFileHasHeader: " + this.inputFileHasHeader + "\r\n\t" +
      "appName: " + this.appName + "\r\n\t" +
      "numPartition: " + this.numPartition + "\r\n\t" +
      "inputFilePartition: " + this.inputFilePartition + "\r\n\t" +
      "inputFromHBase: " + this.inputFromHBase +"\r\n\t" +
      "numAttributes: " + this.numAttributes + "\r\n\t" +
      "sampleThreshold: " + this.efficiencyThreshold + "\r\n\t" +
      "validatorThreshold: " + this.validatorThreshold + "\r\n\t" +
      "useRearrangement: " + this.useRearrangement +"\r\n\t" +
      "scheduleMod: " + this.scheduleMod + "\r\n\t" +
      "useSampler: " + this.useSampler + "\r\n\t" +
      "rearrangeParam: " + this.rearrangeParam + "\r\n\t" +
      "rearrangeUsingRange: " + this.rearrangeUsingRange + "\r\n\t" +
      "numOfThreadUsedInValidator: " + this.numOfThreadUsedInValidator + "\r\n\t" +
      "detectedThreshold: " + this.detectedThreshold + "\r\n\t" +
      "useDetection: " + this.useDetection + "\r\n\t" +
      "detectedToSampleThreshold: " + this.detectedToSampleThreshold + "\r\n\t" +
      "adaptiveSchedulerThreshold: " + this.adaptiveSchedulerThreshold + "\r\n\t" +
      "miniBatchSize:" + this.miniBatchSize + "\r\n\t" +
      "useMultiThreadValidator:" + this.useMultiThreadValidator + "\r\n\t" +
      "numOfThreadUsedInSampler:" + this.numOfThreadUsedInSampler + "\r\n\t" +
      "useMultiThreadSampler:" + this.useMultiThreadSampler + "\r\n\t" +
      "skewThreshold:" + this.skewThreshold + "\r\n\t" +
      "skewDataSize:" + this.skewDataSize + "\r\n\t" +
      "algorithmParallelThreshold" + this.algorithmParallelThreshold + "\r\n\t" +
      "tempFilePath" + this.tempFilePath + "\r\n\t" +
      "writerLocalLogs" + this.writerLocalLogs + "\r\n\t" +
      "localLogDir" + this.localLogDir
  }

}

object Conf {

  def getConf(param: Array[String]): Conf = {
    val conf = new Conf()
    var flag = 0
    for (i <- 0 to param.length - 1) {

     if(i % 2 == 0){
	param(i) match {
        case "--inputFilePath" => {
          conf.inputFilePath = param(i + 1)
          flag += 1
        }
        case "--outputFilePath" => {
          conf.outputFilePath = param(i + 1)
          flag += 1
        }
        case "--inputFileSeparator" => conf.inputFileSeparator = param(i + 1)
        case "--nullEqualsNull" => conf.nullEqualsNull = param(i + 1).toBoolean
        case "--inputFileHasHeader" => conf.inputFileHasHeader = param(i + 1).toBoolean
        case "--appName" => conf.appName = param(i + 1)
        case "--numPartition" => conf.numPartition = param(i + 1).toInt
        case "--inputFromHBase" => conf.inputFromHBase = param(i + 1).toBoolean
        case "--numAttributes" => conf.numAttributes = param(i + 1).toInt
        case "--efficiencyThreshold" => conf.efficiencyThreshold = param(i + 1).toFloat
        case "--validatorThreshold" => conf.validatorThreshold = param(i + 1).toFloat
        case "--useRearrangement" => conf.useRearrangement = param(i + 1).toBoolean
        case "--scheduleMod" => conf.scheduleMod = param(i + 1)
        case "--useSampler" => conf.useSampler = param(i + 1).toBoolean
        case "--rearrangeParam" => conf.rearrangeParam = param(i + 1)
        case "--rearrangeUsingRange" => conf.rearrangeUsingRange = param(i + 1).toBoolean
        case "--numOfThreadUsedInValidator" => conf.numOfThreadUsedInValidator = param(i + 1).toInt
        case "--detectedThreshold" => conf.detectedThreshold = param(i + 1).toInt
        case "--useDetection" => conf.useDetection = param(i + 1).toBoolean
        case "--detectedToSampleThreshold" => conf.detectedToSampleThreshold = param(i + 1).toDouble
        case "--adaptiveSchedulerThreshold" => conf.adaptiveSchedulerThreshold = param(i + 1).toInt
        case "--inputFilePartition" => conf.inputFilePartition = param(i + 1).toInt
        case "--miniBatchSize" => conf.miniBatchSize = param(i + 1).toInt
        case "--useMultiThreadValidator" => conf.useMultiThreadValidator = param(i + 1).toBoolean
        case "--numOfThreadUsedInSampler" => conf.numOfThreadUsedInSampler = param(i + 1).toInt
        case "--useMultiThreadSampler" => conf.useMultiThreadSampler = param(i + 1).toBoolean
        case "--skewThreshold" => conf.skewThreshold = param(i + 1).toDouble
        case "--skewDataSize" => conf.skewDataSize = param(i + 1).toInt
        case "--algorithmParallelThreshold" => conf.algorithmParallelThreshold = param(i + 1).toDouble
        case "--tempFilePath" => conf.tempFilePath = param(i + 1)
        case "--localLogDir" => conf.localLogDir = param(i + 1)
        case "--writerLocalLogs" => conf.writerLocalLogs = param(i + 1).toBoolean
        case _ => {
          println(s"No Such Conf:${param(i)}")
          assert(false, "No Such Conf")
        }
      }
     }
    }
    assert(flag - 2 == 0, "Maybe you forget to set the inputFilePath or outputFilePath")
    conf
  }
}



