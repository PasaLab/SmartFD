package pasa.bigdata.nju.smartfd.structures

import java.util

import scala.collection.mutable

/**
  * FDTree is used to store candidate fds
  */
class FDTree(numAttributes: Int) extends FDTreeElement(numAttributes)  {

  private var depth: Int = 0
  this.children = new Array[FDTreeElement](numAttributes)

  def getDepth: Int = {
    this.depth
  }

  def setDepth(depth: Int): Unit = {
    this.depth = depth
  }

  def init(cardinalityOfAttributes: Array[Int]): Unit = {//only used for create the root of the candidate-fds tree
    this.rhsAttributes.set(0, numAttributes)
    this.rhsFds.set(0, numAttributes)
    for(i<-0 until numAttributes){
      children(i) = new FDTreeElement(numAttributes)
      children(i).rhsAttributes.set(0, numAttributes)
      children(i).rhsAttributes.clear(i)
      children(i).rhsFds.set(0, numAttributes)
      children(i).rhsFds.clear(i)
    }
    setDepth(1)
    for(i <- 0 until numAttributes){
      if(isFd(i) && cardinalityOfAttributes(i) > 1){
        specializePositiveCover(new util.BitSet(numAttributes), i, i)
      }
    }
  }

  /**
    * remove invalid fds from FDTree and add new candidate fds to FDTree
    * @param lhs
    * @param rhs
    * @param currentAttribute
    * @return
    */
  def specializePositiveCover(lhs: util.BitSet,
                              rhs: Int,
                              currentAttribute: Int): Int = {
    val numAttributes: Int = getChildren.length
    var newFDs: Int = 0

    val specLhs = lhs.get(0, numAttributes)
    removeFunctionalDependency(specLhs, rhs)
    for(attr <- (currentAttribute until numAttributes).reverse){
      if (!lhs.get(attr) && (attr != rhs)) {
        specLhs.set(attr)
        val currentAttr = specLhs.nextSetBit(0)
        if(getChildren == null || getChildren(currentAttr) == null){
          if(!isFd(rhs)){
            addFunctionalDependency(specLhs.get(0, numAttributes), rhs)
            newFDs += 1
          }
        }else if(!getChildren(currentAttr).containsFdOrGeneralization(specLhs.get(0, numAttributes), rhs, specLhs.nextSetBit(currentAttr + 1))){
          addFunctionalDependency(specLhs.get(0, numAttributes), rhs)
          newFDs += 1
        }
        specLhs.clear(attr)
      }
    }
    return newFDs
  }

  def getAllFds(lhs: util.BitSet, rhs: Int): util.ArrayList[util.BitSet] = {
    val foundLhs: util.ArrayList[util.BitSet] = new util.ArrayList[util.BitSet]()
    val currentLhs: util.BitSet = new util.BitSet(this.numAttributes)
    val nextLhsAttr: Int = lhs.nextSetBit(0)
    this.getAllFds(lhs, rhs, nextLhsAttr, currentLhs, foundLhs)
    foundLhs
  }


  def getAllFds(lhs: util.BitSet, rhs: Int, currentAttribute: Int): util.ArrayList[util.BitSet] = {
    val foundLhs: util.ArrayList[util.BitSet] = new util.ArrayList[util.BitSet]()
    val currentLhs: util.BitSet = new util.BitSet(this.numAttributes)
    if(this.isFd(rhs)){
      foundLhs.add(currentLhs.get(0, this.numAttributes))
    }

    if(this.children != null && this.children(currentAttribute) != null && this.children(currentAttribute).hasRhsAttribute(rhs)){
      currentLhs.set(currentAttribute)
      val child = this.children(currentAttribute)
      child.getAllFds(lhs, rhs, lhs.nextSetBit(currentAttribute), currentLhs, foundLhs)
    }
    return foundLhs
  }

  def removeFunctionalDependency(lhs: util.BitSet, rhs: Int): Unit = {
    val currentLhsAttr = lhs.nextSetBit(0)
    this.removeRecursive(lhs, rhs, currentLhsAttr)
  }

  def containsFdOrGeneralization(lhs: util.BitSet, rhs: Int): Boolean = {
    val nextLhsAttr: Int = lhs.nextSetBit(0)
    return this.containsFdOrGeneralization(lhs, rhs, nextLhsAttr)
  }

  /**
    * add lhs->rhs to FDTree
    * @param lhs
    * @param rhs
    * @return
    */
  def addFunctionalDependency(lhs: util.BitSet, rhs: Int): FDTreeElement = {
    var currentNode: FDTreeElement = this
    currentNode.addRhsAttribute(rhs)
    var lhsLength: Int = 0

    var i: Int = lhs.nextSetBit(0)
    while (i >= 0) {
      lhsLength += 1
      if (currentNode.getChildren == null) {
        currentNode.setChildren(new Array[FDTreeElement](this.numAttributes))
        currentNode.getChildren(i) = new FDTreeElement(this.numAttributes)
      } else if (currentNode.getChildren(i) == null) {
        currentNode.getChildren(i) = new FDTreeElement(this.numAttributes)
      }

      currentNode = currentNode.getChildren(i)
      currentNode.addRhsAttribute(rhs)
      i = lhs.nextSetBit(i + 1)
    }

    currentNode.markFd(rhs)

    if(currentNode.getChildren != null && currentNode.getChildren.size > 0){
      var j = 0
      while(j < numAttributes){
        val child = currentNode.getChildren(j)
        if(child != null && child.isFd(rhs)){
          removeFD(child, rhs)
          if(child.getFds.cardinality() == 0){
            currentNode.getChildren(j) = null
          }
        }
        j += 1
      }
    }

    this.depth = Math.max(this.depth, lhsLength)
    return currentNode
  }

  def removeFD(fDTree: FDTreeElement, rhs: Int): Unit ={

    fDTree.getFds.clear(rhs)
    fDTree.getRhsAttributes.clear(rhs)
    if(fDTree.getChildren != null && fDTree.getChildren.size > 0){

      var j = 0
      while(j < numAttributes){
        val child = fDTree.getChildren(j)
        if(child != null && child.hasRhsAttribute(rhs)){
          removeFD(child, rhs)
          if(child.getFds.cardinality() == 0){
            fDTree.getChildren(j) = null
          }
        }
        j += 1
      }
    }

  }

  def addFunctionalDependencyGetIfNew(lhs: util.BitSet, rhs: Int): FDTreeElement = {
    var currentNode: FDTreeElement = this
    currentNode.addRhsAttribute(rhs)
    var isNew: Boolean = false
    var lhsLength: Int = 0
    var i: Int = lhs.nextSetBit(0)
    while (i >= 0) {
      lhsLength += 1
      if (currentNode.getChildren == null) {
        currentNode.setChildren(new Array[FDTreeElement](this.numAttributes))
        currentNode.getChildren(i) = new FDTreeElement(this.numAttributes)
        isNew = true
      }
      else if (currentNode.getChildren(i) == null) {
        currentNode.getChildren(i) = new FDTreeElement(this.numAttributes)
        isNew = true
      }
      currentNode = currentNode.getChildren(i)
      currentNode.addRhsAttribute(rhs)
      i = lhs.nextSetBit(i + 1)
    }

    currentNode.markFd(rhs)

    if(currentNode.getChildren != null && currentNode.getChildren.size > 0){
      for(j<-0 until numAttributes){
        val child = currentNode.getChildren(j)
        if(child != null && child.isFd(rhs)){
          removeFD(child, rhs)
          if(child.getFds.cardinality() == 0){
//            child == null
            currentNode.getChildren(j) = null
          }
        }
      }
    }

    this.depth = Math.max(this.depth, lhsLength)
    //this.depth = Math.max(this.depth, lhs.cardinality() + 1)
    if (isNew) return currentNode
    return null
  }

  def getLevel(level: Int,
               distributeAttribute: Int): mutable.HashMap[util.BitSet, FDTreeElementLhsPair] = {
    val result = new mutable.HashMap[util.BitSet, FDTreeElementLhsPair]()
    val currentLhs: util.BitSet = new util.BitSet()
    val currentLevel: Int = 0
    this.getLevel(level, currentLevel, currentLhs, result, distributeAttribute)
    result
  }

  def getLevel(level: Int): util.ArrayList[FDTreeElementLhsPair] = {
    val result = new util.ArrayList[FDTreeElementLhsPair]
    val currentLhs: util.BitSet = new util.BitSet()
    val currentLevel: Int = 0
    this.getLevelNodes(level, currentLevel, currentLhs, result)
    return result
  }

  /**
    * remove non-fds from child distributedAttribute of the fdTree
    *
    * @param distributedAttribute
    */
  def removeNonFDs(distributedAttribute: Int): Unit={
    val rhss: util.BitSet = new util.BitSet(numAttributes)
    if(this.children!=null && this.children(distributedAttribute)!= null){
      this.children(distributedAttribute).removeNonFDs(rhss)
      if(rhss.cardinality() == 0){
        this.children(distributedAttribute) = null
      }
    }
  }
}
