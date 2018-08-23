package pasa.bigdata.nju.smartfd.utils

import java.io.{File, PrintWriter}
import java.util

import pasa.bigdata.nju.smartfd.structures.{FDTree, FDTreeElement}

import scala.collection.mutable.ArrayBuffer

object GetCandidateFDs {


  private def getLhss(currentLhs: util.BitSet,
                      lhsSet: util.HashSet[util.BitSet],
                      fdTree: FDTreeElement,
                      numAttributes: Int): Unit = {
    if(fdTree.getChildren == null){
      val writeLhs:util.BitSet = currentLhs.get(0, numAttributes)
      lhsSet.add(writeLhs)
      return
    }

    val child: Array[FDTreeElement] = fdTree.getChildren

    for(i <- 0 until child.length){
      if(child(i) != null){
        currentLhs.set(i)
        getLhss(currentLhs, lhsSet, child(i), numAttributes)
        currentLhs.clear(i)
      }
    }
  }

  /**
    * Using this method to get the result when we sort the source data by
    * the [cardinality of attribute].
    * @param fdTreePath
    * @param fdTree
    * @param sortedAttributes
    */
  def getMinimalFDsForPar(fdTreePath: String,
                          fdTree: FDTree,
                          sortedAttributes: Array[Int])={
    val numAttributes = fdTree.getNumAttributes
    val fds = new scala.collection.mutable.HashMap[util.BitSet, util.BitSet]()
    val currentLhs: util.BitSet = new util.BitSet(numAttributes)
    val nonRedundantFDs: FDTree = removeRedundantFDs(fdTree)
    getfds(currentLhs, fds, nonRedundantFDs, numAttributes)

    val writer = new PrintWriter(new File(fdTreePath))
    val result = new Array[Int](numAttributes)

    for((lhs, rhss) <- fds){
      var fdString: String = ""
      for(i <- 0 until numAttributes){
        if(lhs.get(i)){
          result(sortedAttributes(i)) = 1
        }else{
          result(sortedAttributes(i)) = 0
        }
      }
      fdString = result.mkString("") + "->"
      for(i <- 0 until numAttributes){
        if(rhss.get(i)){
          result(sortedAttributes(i)) = 1
        }else{
          result(sortedAttributes(i)) = 0
        }
      }

      fdString = fdString + result.mkString("") + "\n"
      writer.write(fdString)
    }
    writer.close()
  }

  /**
    * Using this method to get the result when we sort the source data by the
    * [cardinality of attribute]. This method will write fds to the file named
    * "fdTreePath" using column name.
    * @param fdTreePath
    * @param fdTree
    * @param sortedAttributes
    */
  def getMinimalFDsForParUsingColumnId(fdTreePath: String,
                                       fdTree: FDTree,
                                       sortedAttributes: Array[Int])={
    val numAttributes = fdTree.getNumAttributes
    val fds = new scala.collection.mutable.HashMap[util.BitSet, util.BitSet]()
    val currentLhs: util.BitSet = new util.BitSet(numAttributes)
    val nonRedundantFDs: FDTree = removeRedundantFDs(fdTree)
    getfds(currentLhs, fds, nonRedundantFDs, numAttributes)

    val writer = new PrintWriter(new File(fdTreePath))
    val result = new Array[Int](numAttributes)

    for((lhs, rhss) <- fds){
      var fdString: String = "["
      for(i <- 0 until numAttributes){
        if(lhs.get(i)){
          result(sortedAttributes(i)) = 1
        }else{
          result(sortedAttributes(i)) = 0
        }
      }

      val  lhsResult = for(i <- 0 until result.size  if result(i) != 0)
        yield "column"+(i + 1)

      if(lhsResult == null || lhsResult.size == 0){
        fdString = fdString + "]:"
      }else{
        fdString = fdString+lhsResult.mkString(",") + "]:"
      }
      for(i <- 0 until numAttributes){
        if(rhss.get(i)){
          result(sortedAttributes(i)) = 1
        }else{
          result(sortedAttributes(i)) = 0
        }
      }

      val  rhsResult = for(i <- 0 until result.size  if result(i) != 0)
        yield "column"+(i + 1)

      fdString = fdString + rhsResult.mkString(",")
      writer.println(fdString)
    }
    writer.close()
  }

  /**
    * Using this method to get the result when we sort the source data by the
    * [cardinality of attribute]. This method will write fds to the file named
    * "fdTreePath" using column name.
    * @param fdTree
    * @param sortedAttributes
    */
  def getMinimalFDsForParUsingColumnId(fdTree: FDTree,
                                       sortedAttributes: Array[Int]): Array[String] ={
    val numAttributes = fdTree.getNumAttributes
    val fds = new scala.collection.mutable.HashMap[util.BitSet, util.BitSet]()
    val currentLhs: util.BitSet = new util.BitSet(numAttributes)
    val nonRedundantFDs: FDTree = removeRedundantFDs(fdTree)
    getfds(currentLhs, fds, nonRedundantFDs, numAttributes)
    val totalResult = new ArrayBuffer[String]()
    val result = new Array[Int](numAttributes)

    for((lhs, rhss) <- fds){
      var fdString: String = "["
      for(i <- 0 until numAttributes){
        if(lhs.get(i)){
          result(sortedAttributes(i)) = 1
        }else{
          result(sortedAttributes(i)) = 0
        }
      }

      val  lhsResult = for(i <- 0 until result.size  if result(i) != 0)
        yield "column"+(i + 1)

      if(lhsResult == null || lhsResult.size == 0){
        fdString = fdString + "]:"
      }else{
        fdString = fdString+lhsResult.mkString(",") + "]:"
      }
      for(i <- 0 until numAttributes){
        if(rhss.get(i)){
          result(sortedAttributes(i)) = 1
        }else{
          result(sortedAttributes(i)) = 0
        }
      }

      val  rhsResult = for(i <- 0 until result.size  if result(i) != 0)
        yield "column"+(i + 1)

      fdString = fdString + rhsResult.mkString(",")
      totalResult += fdString
    }
    totalResult.toArray
  }

  private def getfds(currentLhs: util.BitSet,
                     fds: scala.collection.mutable.HashMap[util.BitSet, util.BitSet],
                     fdTree: FDTreeElement,
                     numAttributes: Int): Unit = {
    if(fdTree.getChildren == null){
      val writeLhs:util.BitSet = currentLhs.get(0, numAttributes)
      if(fdTree.getFds.cardinality() != 0){
        fds += (writeLhs -> fdTree.getFds.get(0, numAttributes))
      }
      return
    }

    if(fdTree.getFds.cardinality() != 0){
      val writeLhs:util.BitSet = currentLhs.get(0, numAttributes)
      fds += (writeLhs -> fdTree.getFds.get(0, numAttributes))
    }

    val child: Array[FDTreeElement] = fdTree.getChildren

    for(i <- 0 until child.length){
      if(child(i) != null){
        currentLhs.set(i)
        getfds(currentLhs, fds, child(i), numAttributes)
        currentLhs.clear(i)
      }
    }
  }

  def removeRedundantFDs(posCover: FDTree): FDTree = {
    val nonRedundantFDs = new FDTree(posCover.getNumAttributes)
    val fds = new scala.collection.mutable.HashMap[util.BitSet, util.BitSet]()
    val currentLhs: util.BitSet = new util.BitSet(posCover.getNumAttributes)
    getfds(currentLhs, fds, posCover, posCover.getNumAttributes)
    val sortedFDs = fds.toArray.map(f=>(f._1.cardinality(), f)).sortBy(f=>f._1).map(f=>f._2)
    for(i <- 0 until sortedFDs.length){
      val (lhs, rhss) = sortedFDs(i)
      var rhs = rhss.nextSetBit(0)
      while(rhs >= 0){
        if(!nonRedundantFDs.containsFdOrGeneralization(lhs, rhs)){
          nonRedundantFDs.addFunctionalDependency(lhs, rhs)
        }
        rhs = rhss.nextSetBit(rhs + 1)
      }
    }
    nonRedundantFDs
  }

  private def getFDs(negCover: FDTreeElement): Int = {
    if(negCover == null){
      return 0
    }else{
      var fdNumbers = negCover.getFds.cardinality()
      if(negCover.getChildren != null){
        for(fdTree <- negCover.getChildren){
          fdNumbers += getFDs(fdTree)
        }
      }
      return fdNumbers
    }
  }

  //get the proportion of candidate function dependencies that has been verified
  // the first level fds are not taken account
  def percentOfValidateFD(negCover: FDTree, currentAttribute: Int): Double ={
    var validatorNumber = 0
    var unValidatorNumber = 0
    var index = 0
    if(negCover == null || negCover.getChildren == null){
      return 1.0d
    }else{
      while(index < currentAttribute){
        validatorNumber += getFDs(negCover.getChildren(index))
        index += 1
      }

      while(index < negCover.getNumAttributes){
        unValidatorNumber += getFDs(negCover.getChildren(index))
        index += 1
      }
      return validatorNumber.toDouble / (validatorNumber + unValidatorNumber)
    }
  }
}
