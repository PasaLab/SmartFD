package pasa.bigdata.nju.smartfd.structures

import java.util

import scala.collection.mutable

class FDTreeElement extends Serializable {

  var children: Array[FDTreeElement] = null
  var rhsAttributes: util.BitSet = null
  var rhsFds: util.BitSet = null
  var numAttributes: Int = 0

  def this(numAttributes: Int){
    this()
    this.rhsAttributes = new util.BitSet(numAttributes)
    this.rhsFds = new util.BitSet(numAttributes)
    this.numAttributes = numAttributes

  }

  def getNumAttributes: Int = this.numAttributes

  def getChildren: Array[FDTreeElement] = this.children

  def setChildren(children: Array[FDTreeElement]):Unit = {
    this.children = children
  }

  def getRhsAttributes: util.BitSet = {
    this.rhsAttributes
  }

  def addRhsAttribute(i: Int): Unit = {
    this.rhsAttributes.set(i)
  }

  def addRhsAttributes(other: util.BitSet): Unit = {
    this.rhsAttributes.or(other)
  }

  def  removeRhsAttribute(i: Int): Unit = {
    this.rhsAttributes.clear(i)
  }

  def hasRhsAttribute(i: Int): Boolean = {
    this.rhsAttributes.get(i)
  }

  def getFds: util.BitSet = {
    this.rhsFds
  }

  def markFd(i: Int): Unit = {
    this.rhsFds.set(i)
  }

  def markFds(other: util.BitSet): Unit = {
    this.rhsFds.or(other)
  }

  def removeFd(i: Int): Unit = {
    this.rhsFds.clear(i)
  }

  def removeFds(rhss: util.BitSet): Unit ={
    this.rhsFds.andNot(rhss)
  }

  def retainFds(other: util.BitSet): Unit = {
    this.rhsFds.and(other)
  }

  def setFds(other: util.BitSet): Unit = {
    this.rhsFds = other
  }

  def removeAllFds: Unit = {
    this.rhsFds.clear(0, this.numAttributes)
  }

  def isFd(i: Int): Boolean = {
    this.rhsFds.get(i)
  }

  //get all fds
  def getAllFds(lhs: util.BitSet,
                rhs: Int,
                preCurrentLhsAttr: Int,
                currentLhs: util.BitSet,
                foundLhs: util.ArrayList[util.BitSet]):Unit = {
    var currentLhsAttr = preCurrentLhsAttr
    if(this.isFd(rhs)){
      foundLhs.add(currentLhs.get(0, this.numAttributes))
    }
    if(this.children != null){
      while(currentLhsAttr >= 0){
        val nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1)
        if((this.children(currentLhsAttr) != null)
          && (this.children(currentLhsAttr).hasRhsAttribute(rhs))){
          currentLhs.set(currentLhsAttr)
          this.children(currentLhsAttr).getAllFds(lhs, rhs, nextLhsAttr, currentLhs, foundLhs)
          currentLhs.clear(currentLhsAttr)
        }
        currentLhsAttr = nextLhsAttr
      }
    }else{
      return
    }
  }

  //remove fds
  protected def removeRecursive(lhs: util.BitSet,
                                rhs: Int,
                                currentLhsAttr: Int): Boolean = {
    if(currentLhsAttr < 0) {
      this.removeFd(rhs)
      this.removeRhsAttribute(rhs)
      return true
    }

    if((this.children != null) && (this.children(currentLhsAttr) != null)){
      if(!this.children(currentLhsAttr).removeRecursive(lhs, rhs, lhs.nextSetBit(currentLhsAttr + 1))){
        return false
      }

      if(this.children(currentLhsAttr).getRhsAttributes.cardinality() == 0) {
        this.children(currentLhsAttr) = null
      }
    }

    if(this.isLastNodeOf(rhs)){
      this.removeRhsAttribute(rhs)
      return true
    }
    return false
  }

  //if this node has fds return true, else return false.
  protected def isLastNodeOf(rhs: Int): Boolean = {
    if (this.children == null) return true
    for (i <- 0 until this.children.size){
      if ((this.children(i) != null) && this.children(i).hasRhsAttribute(rhs))
        return false
    }
    return true
  }

  //if the current FDTree contains fd(lhs->rhs) or contains generalizations of fd(lhs->rhs)
  //return true, else return false
   def containsFdOrGeneralization(lhs: util.BitSet,
                                  rhs: Int,
                                  currentLhsAttr: Int): Boolean = {
    if (this.isFd(rhs)) {
      return true
    }

    if (currentLhsAttr < 0) {
      return false
    }

    val nextLhsAttr: Int = lhs.nextSetBit(currentLhsAttr + 1)

    if ((this.children != null) && (this.children(currentLhsAttr) != null)
      && (this.children(currentLhsAttr).hasRhsAttribute(rhs))){
      if (this.children(currentLhsAttr).containsFdOrGeneralization(lhs, rhs, nextLhsAttr)) {
        return true
      }
    }
    return this.containsFdOrGeneralization(lhs, rhs, nextLhsAttr)
  }

  /**
    * get all nodes in the fdTree of layer [level].
    *
    * @param level nodes layer
    * @param currentLevel current traversal layer
    * @param currentLhs current traversal lhs
    * @param result
    * @param distributeAttribute
    */
  def getLevel (level: Int,
                currentLevel: Int,
                currentLhs: util.BitSet,
                result: mutable.HashMap[util.BitSet, FDTreeElementLhsPair],
                distributeAttribute: Int): Unit = {
    if (level == currentLevel) {
      result += (currentLhs.get(0, numAttributes) -> new FDTreeElementLhsPair(this, currentLhs.get(0, numAttributes), level))
    }
    else {
      if(currentLevel == 0){
        if(this.children != null && this.children(distributeAttribute) != null){
          currentLhs.set(distributeAttribute)
          this.children(distributeAttribute).getLevel(level, currentLevel + 1, currentLhs, result, distributeAttribute)
          currentLhs.clear(distributeAttribute)
        }
      }else{
        if (this.children != null){
          for (child <- 0 until this.numAttributes) {
            if (this.children(child) != null){
              currentLhs.set(child)
              this.children(child).getLevel(level, currentLevel+1, currentLhs, result, distributeAttribute)
              currentLhs.clear(child)
            }
          }
        }
      }
    }
  }

  //get all of the nodes of a specific level
  protected def getLevelNodes(level: Int,
                              currentLevel: Int,
                              currentLhs: util.BitSet,
                              result: util.ArrayList[FDTreeElementLhsPair]): Unit = {
    if (level == currentLevel) {
      result.add(new FDTreeElementLhsPair(this, currentLhs.get(0, numAttributes), currentLevel))
    }
    else {
        if (this.children == null) return
        for (child <- 0 until this.numAttributes) {
            if (this.children(child) != null){
              currentLhs.set(child)
              this.children(child).getLevelNodes(level, currentLevel + 1, currentLhs, result)
              currentLhs.clear(child)
            }
        }
    }
  }

  /**
    * remove all of the non-FDs from fdTree
    *
    * @param rhss
    */
  def removeNonFDs(rhss: util.BitSet): Unit ={
    if(this.children == null){
      this.rhsAttributes = this.getFds.get(0, numAttributes)
      rhss.or(this.getFds)
    }else{
      this.rhsAttributes = this.getFds.get(0, numAttributes)
      for(i<- 0 until this.numAttributes){
        if(this.children(i) != null){
          this.children(i).removeNonFDs(rhss)
        }
        if(rhss.cardinality() == 0){
          this.children(i) = null
        }
        this.rhsAttributes.or(rhss)
        rhss.clear(0, numAttributes)
      }
      var flag = false
      for(i <- 0 until numAttributes){
        if(this.children(i)!= null){
          flag = true
        }
      }
      if(!flag){
        this.children = null
      }
      rhss.or(this.getRhsAttributes)
    }
  }
}
