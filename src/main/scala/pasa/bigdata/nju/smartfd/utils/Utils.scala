package pasa.bigdata.nju.smartfd.utils

import java.util


object Utils {

  def bitSetToString(bitSet: util.BitSet, numAttributes: Int): String = {
    var result = ""
    for(i<- 0 until numAttributes){
      if(bitSet.get(i)){
        result += "1"
      }else{
        result += "0"
      }
    }
    result
  }

}
